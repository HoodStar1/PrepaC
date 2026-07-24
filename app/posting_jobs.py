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

ACTIVE_POSTING_PROCS = {}

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

_POSTING_EVENT_THROTTLE_STATE = TTLDict(ttl_seconds=3600)

def now():
    return local_now_iso()

def create_posting_job(job_name, packed_root, output_files_root, template_path, size_bytes=0, idempotency_key=None, return_created=False):
    conn = get_conn(); cur = conn.cursor()
    key = str(idempotency_key or "").strip() or None
    try:
        conn.execute("BEGIN IMMEDIATE")
        if key:
            row = cur.execute(
                "SELECT id FROM posting_jobs "
                "WHERE (idempotency_key=? OR packed_root=?) "
                "AND status IN ('queued','running','finalizing','outcome_unknown') "
                "ORDER BY id DESC LIMIT 1",
                (key, packed_root),
            ).fetchone()
        else:
            row = cur.execute(
                "SELECT id FROM posting_jobs WHERE packed_root=? "
                "AND status IN ('queued','running','finalizing','outcome_unknown') "
                "ORDER BY id DESC LIMIT 1",
                (packed_root,),
            ).fetchone()
        if row:
            conn.commit()
            result = int(row[0])
            return (result, False) if return_created else result
        cur.execute(
            "INSERT INTO posting_jobs(job_name, packed_root, output_files_root, template_path, size_bytes, status, created_at, idempotency_key) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)",
            (job_name, packed_root, output_files_root, template_path, int(size_bytes or 0), now(), key),
        )
        job_id = cur.lastrowid
        conn.commit()
        return (job_id, True) if return_created else job_id
    finally:
        conn.close()



def get_existing_active_posting_job_id(packed_root):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM posting_jobs WHERE packed_root=? AND status IN ('queued','running','finalizing','outcome_unknown') ORDER BY id DESC LIMIT 1", (packed_root,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None



def latest_successful_posting_job_id(packed_root):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM posting_jobs WHERE packed_root=? AND status='done' ORDER BY id DESC LIMIT 1", (packed_root,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None


def reconcile_orphaned_running_posting_jobs(active_job_ids, reason="Recovered orphaned posting job with no active worker thread"):
    active_ids = set()
    for value in active_job_ids or set():
        try:
            active_ids.add(int(value))
        except Exception:
            pass
    try:
        stale_min_age = max(300, int(str(os.environ.get("PREPAC_POSTING_RECOVERY_MIN_AGE_SECONDS", "1200") or "1200")))
    except Exception:
        stale_min_age = 1200
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status, percent, started_at, created_at
        FROM posting_jobs
        WHERE status IN ('running','finalizing')
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    events_by_job = recent_event_timestamps(
        cur,
        "posting_job_events",
        "posting_job_id",
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
            recovered_reason += "; remote posting/finalization may have completed, so verify before force retry"
        cur.execute(
            "UPDATE posting_jobs SET status=?, finished_at=?, message=?, provider_used='', provider_lock='' WHERE id=? AND status=?",
            (target_status, now(), recovered_reason, job_id, prior_status),
        )
        if cur.rowcount <= 0:
            continue
        cur.execute("INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (job_id, now(), ('outcome_unknown' if target_status == 'outcome_unknown' else 'recovered'), recovered_reason, row.get('percent')))
        changed += 1
    conn.commit(); conn.close()
    return changed

def latest_successful_posting_finished_at(packed_root):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT finished_at FROM posting_jobs WHERE packed_root=? AND status='done' ORDER BY id DESC LIMIT 1", (packed_root,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else ""

def has_outdated_or_missing_successful_posting(packed_root, packed_finished_at=""):
    latest_post = latest_successful_posting_finished_at(packed_root)
    if not latest_post:
        return True
    if not packed_finished_at:
        return False
    packed_at = parse_local_timestamp(packed_finished_at)
    posted_at = parse_local_timestamp(latest_post)
    if packed_at is None or posted_at is None:
        return True
    return packed_at > posted_at

def update_posting_job(job_id, **fields):
    if not fields:
        return
    conn = get_conn(); cur = conn.cursor()
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [job_id]
    cur.execute(f"UPDATE posting_jobs SET {cols} WHERE id=?", vals)
    conn.commit(); conn.close()


def reset_posting_to_queued_if_active(job_id):
    """Clear a provider claim without reviving a concurrently cancelled job."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE posting_jobs SET status='queued', provider_used='', provider_lock='' "
        "WHERE id=? AND status IN ('queued','running')",
        (int(job_id),),
    )
    changed = cur.rowcount > 0
    conn.commit(); conn.close()
    return changed

def add_posting_event(job_id, phase, message, percent=None):
    phase_norm = str(phase or "").strip().lower()
    message_norm = str(message or "").strip()
    throttle_window_seconds = 0.0
    if phase_norm in {"queued", "posting", "finalizing", "postcheck"}:
        throttle_window_seconds = 1.5
    if throttle_window_seconds > 0:
        key = (int(job_id), phase_norm, message_norm, str(percent))
        now_mono = time.monotonic()
        last = _POSTING_EVENT_THROTTLE_STATE.get(key, 0.0)
        if (now_mono - last) < throttle_window_seconds:
            return
        _POSTING_EVENT_THROTTLE_STATE[key] = now_mono
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
        (job_id, now(), phase, message, percent)
    )
    cur.execute("DELETE FROM posting_job_events WHERE posting_job_id=? AND id NOT IN (SELECT id FROM posting_job_events WHERE posting_job_id=? ORDER BY id DESC LIMIT 100)", (job_id, job_id))
    cur.execute("UPDATE posting_jobs SET phase=?, percent=?, message=? WHERE id=?", (phase, percent, message, job_id))
    conn.commit(); conn.close()



def count_running_posting_jobs():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM posting_jobs WHERE status IN ('running','finalizing')")
    row = cur.fetchone()
    conn.close()
    return int(row[0] or 0)

def try_claim_posting_provider(job_id, provider_name, provider_lock=None):
    provider_name = str(provider_name or "").strip()
    if not provider_name:
        return False
    provider_lock = str(provider_lock or provider_name).strip() or provider_name
    conn = get_conn(); cur = conn.cursor()
    try:
        # Set a timeout to prevent hanging on database locks
        conn.execute("PRAGMA busy_timeout = 5000")  # 5 second timeout
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            "SELECT COUNT(*) FROM posting_jobs "
            "WHERE status IN ('running','finalizing') AND COALESCE(NULLIF(provider_lock, ''), provider_used)=?",
            (provider_lock,),
        )
        running = int(cur.fetchone()[0] or 0)
        if running > 0:
            conn.rollback()
            conn.close()
            return False
        cur.execute("SELECT id FROM posting_jobs WHERE status='queued' ORDER BY id ASC LIMIT 1")
        next_row = cur.fetchone()
        if not next_row or int(next_row[0]) != int(job_id):
            conn.rollback()
            conn.close()
            return False
        cur.execute(
            "UPDATE posting_jobs SET status='running', started_at=?, provider_used=?, provider_lock=? WHERE id=? AND status='queued'",
            (now(), provider_name, provider_lock, job_id),
        )
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
        logging.getLogger(__name__).warning(f"Failed to claim posting provider {provider_name} for job {job_id}: {e}")
        return False

def start_posting(job_id, provider_used=None, provider_lock=None):
    fields = {"status":"running", "started_at":now()}
    if provider_used:
        fields["provider_used"] = provider_used
        fields["provider_lock"] = str(provider_lock or provider_used)
    update_posting_job(job_id, **fields)

def finish_posting(job_id, success=True, message=""):
    conn = get_conn(); cur = conn.cursor()
    eligible_statuses = "('running','finalizing')" if success else "('queued','running')"
    cur.execute(
        "UPDATE posting_jobs SET status=?, finished_at=?, message=?, percent=? "
        f"WHERE id=? AND status IN {eligible_statuses}",
        ("done" if success else "failed", now(), message, 100 if success else None, job_id),
    )
    conn.commit(); conn.close()


def begin_posting_finalization(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE posting_jobs SET status='finalizing', phase='finalizing' WHERE id=? AND status='running'", (int(job_id),))
    changed = cur.rowcount > 0
    conn.commit(); conn.close()
    return changed


def mark_posting_outcome_unknown(job_id, message):
    safe_message = str(message or "Posting finalization outcome is unknown")[:1000]
    conn = get_conn(); cur = conn.cursor()
    try:
        timestamp = now()
        cur.execute(
            "UPDATE posting_jobs SET status='outcome_unknown', phase='outcome_unknown', "
            "finished_at=?, message=?, percent=NULL, provider_used='', provider_lock='' "
            "WHERE id=? AND status='finalizing'",
            (timestamp, safe_message, int(job_id)),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'outcome_unknown', ?, NULL)",
                (int(job_id), timestamp, safe_message),
            )
        conn.commit()
        return changed
    finally:
        conn.close()

def list_posting_jobs(limit=200):
    """Fetch posting jobs with events - optimized to avoid N+1 queries."""
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch jobs
    cur.execute("SELECT * FROM posting_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all events in a single query
    placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT posting_job_id, phase, message, percent, timestamp, id 
        FROM posting_job_events 
        WHERE posting_job_id IN ({placeholders})
        ORDER BY posting_job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id
    events_by_job = {}
    for event in all_events:
        job_id = event["posting_job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 50:
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


def list_posting_jobs_by_status(statuses, limit=500):
    """Fetch posting jobs by status with events - optimized to avoid N+1 queries."""
    wanted = [str(s).strip().lower() for s in (statuses or []) if str(s).strip()]
    if not wanted:
        return []
    
    placeholders = ",".join("?" for _ in wanted)
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch jobs
    cur.execute(f"SELECT * FROM posting_jobs WHERE lower(status) IN ({placeholders}) ORDER BY id DESC LIMIT ?", 
                tuple(wanted) + (int(limit),))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all events in a single query
    event_placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT posting_job_id, phase, message, percent, timestamp, id 
        FROM posting_job_events 
        WHERE posting_job_id IN ({event_placeholders})
        ORDER BY posting_job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id
    events_by_job = {}
    for event in all_events:
        job_id = event["posting_job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 50:
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

def list_posting_history(limit=1000):
    return list_posting_jobs(limit)

def has_successful_posting(job_name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM posting_jobs WHERE job_name=? AND status='done' LIMIT 1", (job_name,))
    row = cur.fetchone()
    conn.close(); return row is not None

def get_running_provider_names():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT provider_used FROM posting_jobs WHERE status IN ('running','finalizing') AND provider_used IS NOT NULL AND provider_used != ''")
    vals = [r[0] for r in cur.fetchall()]
    conn.close(); return vals

def has_large_queued_posting_job(min_size_bytes, exclude_job_id=None):
    min_size_bytes = int(min_size_bytes or 0)
    exclude_job_id = int(exclude_job_id or 0)
    conn = get_conn(); cur = conn.cursor()
    if exclude_job_id > 0:
        cur.execute(
            "SELECT 1 FROM posting_jobs WHERE status='queued' AND size_bytes >= ? AND id != ? LIMIT 1",
            (min_size_bytes, exclude_job_id),
        )
    else:
        cur.execute(
            "SELECT 1 FROM posting_jobs WHERE status='queued' AND size_bytes >= ? LIMIT 1",
            (min_size_bytes,),
        )
    row = cur.fetchone()
    conn.close()
    return row is not None


def interrupt_running_posting_jobs(reason="Interrupted by container shutdown", recovery=False):
    for proc in list(ACTIVE_POSTING_PROCS.values()):
        try:
            terminate_process(proc)
        except Exception:
            pass
    if not recovery:
        # Do not race a still-running worker's final database update.
        return len(ACTIVE_POSTING_PROCS)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM posting_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        phase = "recovered" if recovery else "shutdown"
        message = ("Recovered after restart: previous container exited during job execution"
                   if recovery else reason)
        cur.execute("UPDATE posting_jobs SET status='failed', finished_at=?, message=?, provider_used='', provider_lock='' WHERE id=?",
                    (now(), message, row["id"]))
        cur.execute("INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (row["id"], now(), phase, message, row.get("percent")))
    conn.commit(); conn.close()
    return len(rows)


def acknowledge_posting_outcome_unknown_for_resubmission(job_id):
    """Unblock a fresh posting submission after remote/local reconciliation."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE posting_jobs SET status='failed', phase='manual_reconcile', "
            "message='Unknown outcome manually reconciled; fresh submission permitted', "
            "provider_used='', provider_lock='' "
            "WHERE id=? AND status='outcome_unknown'",
            (int(job_id),),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'manual_reconcile', 'Unknown outcome manually reconciled; fresh submission permitted', NULL)",
                (int(job_id), now()),
            )
        conn.commit()
        return changed
    finally:
        conn.close()


def get_posting_job_status(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT status FROM posting_jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return str(row[0]) if row else ""

def register_posting_proc(job_id, proc):
    ACTIVE_POSTING_PROCS[int(job_id)] = proc

def unregister_posting_proc(job_id, proc=None):
    current = ACTIVE_POSTING_PROCS.get(int(job_id))
    if proc is None or current is proc:
        ACTIVE_POSTING_PROCS.pop(int(job_id), None)

def cancel_posting_job(job_id, reason="Cancelled by user"):
    proc = ACTIVE_POSTING_PROCS.get(int(job_id))
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE posting_jobs SET status='cancelled', finished_at=?, message=?, provider_used='', provider_lock='' WHERE id=? AND status IN ('queued','running')", (now(), reason, job_id))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    if changed and proc is not None:
        try:
            terminate_process(proc)
        except Exception:
            pass
    return changed
