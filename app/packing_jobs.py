import os
import time
from app.db import get_conn
from app.subprocess_utils import terminate_process
from app.timestamp_utils import (
    latest_local_timestamp,
    local_now,
    local_now_iso,
    parse_local_timestamp,
    recent_event_timestamps,
)

ACTIVE_PACKING_PROCS = {}

class TTLDict:
    """Dictionary with time-to-live for entries to prevent unbounded growth."""
    def __init__(self, ttl_seconds=3600):
        self.ttl = ttl_seconds
        self.data = {}
    
    def get(self, key, default=None):
        """Get value, cleaning expired entries."""
        self._cleanup()
        value, _ = self.data.get(key, (default, None))
        return value
    
    def __getitem__(self, key):
        """Dict-style access."""
        self._cleanup()
        return self.data[key][0]
    
    def __setitem__(self, key, value):
        """Dict-style setting."""
        self._cleanup()
        self.data[key] = (value, time.time())
    
    def _cleanup(self):
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, (_, ts) in self.data.items() if now - ts > self.ttl]
        for k in expired:
            del self.data[k]

_PACKING_EVENT_THROTTLE_STATE = TTLDict(ttl_seconds=3600)

def now():
    return local_now_iso()

def create_packing_job(source_path, job_name, output_root, output_files_root, idempotency_key=None, return_created=False):
    conn = get_conn(); cur = conn.cursor()
    key = str(idempotency_key or "").strip() or None
    try:
        conn.execute("BEGIN IMMEDIATE")
        if key:
            row = cur.execute(
                "SELECT id FROM packing_jobs "
                "WHERE (idempotency_key=? OR source_path=?) "
                "AND status IN ('queued','running','finalizing','outcome_unknown') "
                "ORDER BY id DESC LIMIT 1",
                (key, source_path),
            ).fetchone()
        else:
            row = cur.execute(
                "SELECT id FROM packing_jobs WHERE source_path=? "
                "AND status IN ('queued','running','finalizing','outcome_unknown') "
                "ORDER BY id DESC LIMIT 1",
                (source_path,),
            ).fetchone()
        if row:
            conn.commit()
            result = int(row[0])
            return (result, False) if return_created else result
        cur.execute(
            "INSERT INTO packing_jobs(source_path, job_name, output_root, output_files_root, status, created_at, idempotency_key) VALUES (?, ?, ?, ?, 'queued', ?, ?)",
            (source_path, job_name, output_root, output_files_root, now(), key),
        )
        job_id = cur.lastrowid
        conn.commit()
        return (job_id, True) if return_created else job_id
    finally:
        conn.close()



def get_existing_active_packing_job_id(source_path):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM packing_jobs WHERE source_path=? AND status IN ('queued','running','finalizing','outcome_unknown') ORDER BY id DESC LIMIT 1", (source_path,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None



def latest_successful_packing_job_id(source_path):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM packing_jobs WHERE source_path=? AND status='done' ORDER BY id DESC LIMIT 1", (source_path,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None


def reconcile_orphaned_running_packing_jobs(active_job_ids, reason="Recovered orphaned packing job with no active worker thread"):
    active_ids = set()
    for value in active_job_ids or set():
        try:
            active_ids.add(int(value))
        except Exception:
            pass
    try:
        stale_min_age = max(300, int(str(os.environ.get("PREPAC_PACKING_RECOVERY_MIN_AGE_SECONDS", "2700") or "2700")))
    except Exception:
        stale_min_age = 2700
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status, percent, started_at, created_at
        FROM packing_jobs
        WHERE status IN ('running','finalizing')
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    events_by_job = recent_event_timestamps(
        cur,
        "packing_job_events",
        "packing_job_id",
        (row["id"] for row in rows),
    )
    changed = 0
    now_dt = local_now()
    for row in rows:
        job_id = int(row.get("id") or 0)
        if job_id in active_ids:
            continue
        last_activity = latest_local_timestamp(
            [
                row.get("started_at"),
                row.get("created_at"),
                *events_by_job.get(job_id, []),
            ]
        )
        if last_activity:
            age_seconds = int((now_dt - last_activity).total_seconds())
            if age_seconds < stale_min_age:
                continue
        prior_status = str(row.get("status") or "").lower()
        target_status = "outcome_unknown" if prior_status == "finalizing" else "failed"
        recovered_reason = f"{reason}; no persisted activity for at least {stale_min_age}s"
        if target_status == "outcome_unknown":
            recovered_reason += "; finalization may have completed, so verify outputs before force retry"
        cur.execute(
            "UPDATE packing_jobs SET status=?, finished_at=?, message=? WHERE id=? AND status=?",
            (target_status, now(), recovered_reason, job_id, prior_status),
        )
        if cur.rowcount <= 0:
            continue
        cur.execute("INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (job_id, now(), ('outcome_unknown' if target_status == 'outcome_unknown' else 'recovered'), recovered_reason, row.get('percent')))
        changed += 1
    conn.commit(); conn.close()
    return changed

def latest_successful_packing_finished_at(source_path):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT finished_at FROM packing_jobs WHERE source_path=? AND status='done' ORDER BY id DESC LIMIT 1", (source_path,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else ""

def has_outdated_or_missing_successful_packing(source_path, prepared_finished_at=""):
    latest_pack = latest_successful_packing_finished_at(source_path)
    if not latest_pack:
        return True
    if not prepared_finished_at:
        return False
    prepared_at = parse_local_timestamp(prepared_finished_at)
    packed_at = parse_local_timestamp(latest_pack)
    if prepared_at is None or packed_at is None:
        return True
    return prepared_at > packed_at

def count_running_packing_jobs():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM packing_jobs WHERE status IN ('running','finalizing')")
    row = cur.fetchone()
    conn.close()
    return int(row[0] or 0)



def try_claim_packing_slot(job_id, max_jobs):
    conn = get_conn(); cur = conn.cursor()
    try:
        # Set a timeout to prevent hanging on database locks
        conn.execute("PRAGMA busy_timeout = 5000")  # 5 second timeout
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT COUNT(*) FROM packing_jobs WHERE status IN ('running','finalizing')")
        running = int(cur.fetchone()[0] or 0)
        if running >= int(max_jobs):
            conn.rollback()
            conn.close()
            return False
        cur.execute("SELECT id FROM packing_jobs WHERE status='queued' ORDER BY id ASC LIMIT 1")
        next_row = cur.fetchone()
        if not next_row or int(next_row[0]) != int(job_id):
            conn.rollback()
            conn.close()
            return False
        cur.execute("UPDATE packing_jobs SET status='running', started_at=? WHERE id=? AND status='queued'", (now(), job_id))
        claimed = cur.rowcount == 1
        conn.commit()
        conn.close()
        return claimed
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
        import logging
        logging.getLogger(__name__).warning(f"Failed to claim packing slot for job {job_id}: {e}")
        return False

def update_packing_job(job_id, **fields):
    if not fields:
        return
    conn = get_conn(); cur = conn.cursor()
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [job_id]
    cur.execute(f"UPDATE packing_jobs SET {cols} WHERE id=?", vals)
    conn.commit(); conn.close()

def add_packing_event(job_id, phase, message, percent=None):
    phase_norm = str(phase or "").strip().lower()
    throttle_window_seconds = 0.0
    if phase_norm in {"queued", "rar", "par2", "stability"}:
        throttle_window_seconds = 1.0
    if throttle_window_seconds > 0:
        # Throttle by job+phase+percent band to keep useful progress while avoiding spam.
        percent_band = None if percent is None else int(percent)
        key = (int(job_id), phase_norm, percent_band)
        now_mono = time.monotonic()
        last = _PACKING_EVENT_THROTTLE_STATE.get(key, 0.0)
        if (now_mono - last) < throttle_window_seconds:
            return
        _PACKING_EVENT_THROTTLE_STATE[key] = now_mono
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
        (job_id, now(), phase, message, percent)
    )
    cur.execute("DELETE FROM packing_job_events WHERE packing_job_id=? AND id NOT IN (SELECT id FROM packing_job_events WHERE packing_job_id=? ORDER BY id DESC LIMIT 100)", (job_id, job_id))
    cur.execute("UPDATE packing_jobs SET phase=?, percent=?, message=? WHERE id=?", (phase, percent, message, job_id))
    conn.commit(); conn.close()

def start_packing(job_id):
    """Claim a queued job without reviving a cancelled or terminal row."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE packing_jobs SET status='running', started_at=COALESCE(started_at, ?) "
        "WHERE id=? AND status='queued' AND output_reset_claimed_at IS NULL",
        (now(), int(job_id)),
    )
    changed = cur.rowcount == 1
    conn.commit(); conn.close()
    return changed


def begin_packing_output_reset(job_id):
    """Atomically claim ownership before any existing output is removed.

    Cancellation uses the inverse compare-and-swap predicate. SQLite serializes
    the two writes, so either cancellation wins and this returns false, or this
    marker wins and cancellation remains unavailable for this packing attempt.
    """
    timestamp = now()
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE packing_jobs SET output_reset_claimed_at=?, status='running', "
        "started_at=COALESCE(started_at, ?), phase='resetting', "
        "message='Claimed packing output; preparing clean output directories' "
        "WHERE id=? AND status IN ('queued','running') "
        "AND output_reset_claimed_at IS NULL",
        (timestamp, timestamp, int(job_id)),
    )
    changed = cur.rowcount == 1
    conn.commit(); conn.close()
    return changed

def finish_packing(job_id, success=True, message=""):
    conn = get_conn(); cur = conn.cursor()
    eligible_statuses = "('running','finalizing')" if success else "('queued','running')"
    cur.execute(
        "UPDATE packing_jobs SET status=?, finished_at=?, message=?, percent=? "
        f"WHERE id=? AND status IN {eligible_statuses}",
        ("done" if success else "failed", now(), message, 100 if success else None, job_id),
    )
    conn.commit(); conn.close()


def begin_packing_finalization(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE packing_jobs SET status='finalizing', phase='finalizing' "
        "WHERE id=? AND status='running' AND output_reset_claimed_at IS NOT NULL",
        (int(job_id),),
    )
    changed = cur.rowcount > 0
    conn.commit(); conn.close()
    return changed


def mark_packing_outcome_unknown(job_id, message):
    safe_message = str(message or "Packing finalization outcome is unknown")[:1000]
    conn = get_conn(); cur = conn.cursor()
    try:
        timestamp = now()
        cur.execute(
            "UPDATE packing_jobs SET status='outcome_unknown', phase='outcome_unknown', "
            "finished_at=?, message=?, percent=NULL WHERE id=? AND status='finalizing'",
            (timestamp, safe_message, int(job_id)),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'outcome_unknown', ?, NULL)",
                (int(job_id), timestamp, safe_message),
            )
        conn.commit()
        return changed
    finally:
        conn.close()

def list_packing_jobs(limit=200):
    """Fetch packing jobs with events - optimized to avoid N+1 queries."""
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch jobs
    cur.execute("SELECT * FROM packing_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all events in a single query
    placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT packing_job_id, phase, message, percent, timestamp, id 
        FROM packing_job_events 
        WHERE packing_job_id IN ({placeholders})
        ORDER BY packing_job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id
    events_by_job = {}
    for event in all_events:
        job_id = event["packing_job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 20:
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
    
    return jobs


def list_packing_jobs_by_status(statuses, limit=500):
    """Fetch packing jobs by status with events - optimized to avoid N+1 queries."""
    wanted = [str(s).strip().lower() for s in (statuses or []) if str(s).strip()]
    if not wanted:
        return []
    
    placeholders = ",".join("?" for _ in wanted)
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch jobs
    cur.execute(f"SELECT * FROM packing_jobs WHERE lower(status) IN ({placeholders}) ORDER BY id DESC LIMIT ?", 
                tuple(wanted) + (int(limit),))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all events in a single query
    event_placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT packing_job_id, phase, message, percent, timestamp, id 
        FROM packing_job_events 
        WHERE packing_job_id IN ({event_placeholders})
        ORDER BY packing_job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id
    events_by_job = {}
    for event in all_events:
        job_id = event["packing_job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 20:
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
    
    return jobs

def has_successful_packing(source_path):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM packing_jobs WHERE source_path=? AND status='done' LIMIT 1", (source_path,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def list_packing_history(limit=1000):
    """Fetch packing history with events - optimized to avoid N+1 queries."""
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch jobs
    cur.execute("SELECT * FROM packing_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all events in a single query
    placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT packing_job_id, phase, message, percent, timestamp, id 
        FROM packing_job_events 
        WHERE packing_job_id IN ({placeholders})
        ORDER BY packing_job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id
    events_by_job = {}
    for event in all_events:
        job_id = event["packing_job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 50:  # Keep 50 for history
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
    
    return jobs


def interrupt_running_packing_jobs(reason="Interrupted by container shutdown", recovery=False):
    for proc in list(ACTIVE_PACKING_PROCS.values()):
        try:
            terminate_process(proc)
        except Exception:
            pass
    if not recovery:
        # The worker thread may still complete after its child process exits.
        # Leave persisted state untouched; stale reconciliation will decide the
        # outcome after this process has actually gone away.
        return len(ACTIVE_PACKING_PROCS)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM packing_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        phase = "recovered" if recovery else "shutdown"
        message = ("Recovered after restart: previous container exited during job execution"
                   if recovery else reason)
        cur.execute("UPDATE packing_jobs SET status='failed', finished_at=?, message=? WHERE id=?",
                    (now(), message, row["id"]))
        cur.execute("INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (row["id"], now(), phase, message, row.get("percent")))
    conn.commit(); conn.close()
    return len(rows)


def acknowledge_packing_outcome_unknown_for_resubmission(job_id):
    """Unblock a fresh packing submission after output/source reconciliation."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE packing_jobs SET status='failed', phase='manual_reconcile', "
            "message='Unknown outcome manually reconciled; fresh submission permitted' "
            "WHERE id=? AND status='outcome_unknown'",
            (int(job_id),),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'manual_reconcile', 'Unknown outcome manually reconciled; fresh submission permitted', NULL)",
                (int(job_id), now()),
            )
        conn.commit()
        return changed
    finally:
        conn.close()


def has_large_running_packing_job(min_size_bytes):
    min_size_bytes = int(min_size_bytes or 0)
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM packing_jobs WHERE status IN ('running','finalizing') AND size_bytes >= ? LIMIT 1",
        (min_size_bytes,),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_packing_job_status(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT status FROM packing_jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return str(row[0]) if row else ""

def register_packing_proc(job_id, proc):
    ACTIVE_PACKING_PROCS[int(job_id)] = proc

def unregister_packing_proc(job_id, proc=None):
    current = ACTIVE_PACKING_PROCS.get(int(job_id))
    if proc is None or current is proc:
        ACTIVE_PACKING_PROCS.pop(int(job_id), None)

def cancel_packing_job(job_id, reason="Cancelled by user"):
    proc = ACTIVE_PACKING_PROCS.get(int(job_id))
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE packing_jobs SET status='cancelled', finished_at=?, message=? "
        "WHERE id=? AND status IN ('queued','running') "
        "AND output_reset_claimed_at IS NULL",
        (now(), reason, int(job_id)),
    )
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    if changed and proc is not None:
        try:
            terminate_process(proc)
        except Exception:
            pass
    return changed
