from datetime import datetime
import time
from app.db import get_conn

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
    return datetime.now().isoformat(timespec="seconds")

def create_packing_job(source_path, job_name, output_root, output_files_root):
    existing_id = get_existing_active_packing_job_id(source_path)
    if existing_id:
        return existing_id
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO packing_jobs(source_path, job_name, output_root, output_files_root, status, created_at) VALUES (?, ?, ?, ?, 'queued', ?)",
        (source_path, job_name, output_root, output_files_root, now())
    )
    job_id = cur.lastrowid
    conn.commit(); conn.close()
    return job_id



def get_existing_active_packing_job_id(source_path):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM packing_jobs WHERE source_path=? AND status IN ('queued','running') ORDER BY id DESC LIMIT 1", (source_path,))
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
    active_ids = {int(x) for x in (active_job_ids or set()) if str(x).isdigit()}
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, percent FROM packing_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    changed = 0
    for row in rows:
        job_id = int(row.get("id") or 0)
        if job_id in active_ids:
            continue
        cur.execute("UPDATE packing_jobs SET status='failed', finished_at=?, message=? WHERE id=?", (now(), reason, job_id))
        cur.execute("INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (job_id, now(), 'recovered', reason, row.get('percent')))
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
    try:
        return prepared_finished_at > latest_pack
    except Exception:
        return prepared_finished_at != latest_pack

def count_running_packing_jobs():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM packing_jobs WHERE status='running'")
    row = cur.fetchone()
    conn.close()
    return int(row[0] or 0)



def try_claim_packing_slot(job_id, max_jobs):
    conn = get_conn(); cur = conn.cursor()
    try:
        # Set a timeout to prevent hanging on database locks
        conn.execute("PRAGMA busy_timeout = 5000")  # 5 second timeout
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT COUNT(*) FROM packing_jobs WHERE status='running'")
        running = int(cur.fetchone()[0] or 0)
        if running >= int(max_jobs):
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
    update_packing_job(job_id, status="running", started_at=now())

def finish_packing(job_id, success=True, message=""):
    update_packing_job(job_id, status="done" if success else "failed", finished_at=now(), message=message, percent=100 if success else None)

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


def has_large_running_packing_job(min_size_bytes):
    min_size_bytes = int(min_size_bytes or 0)
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM packing_jobs WHERE status='running' AND size_bytes >= ? LIMIT 1",
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
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE packing_jobs SET status='cancelled', finished_at=?, message=? WHERE id=? AND status IN ('queued','running')", (now(), reason, job_id))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    return changed
