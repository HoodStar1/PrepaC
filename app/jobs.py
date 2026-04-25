from datetime import datetime
import subprocess
import os
from app.db import get_conn

ACTIVE_PREPARE_PROCS = {}
ACTIVE_PREPARE_WORKERS = set()

def now(): return datetime.now().isoformat(timespec="seconds")

def create_job(media_type, source_path, dest_path=""):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO prepare_jobs(media_type, status, source_path, dest_path, started_at) VALUES (?, 'queued', ?, ?, ?)",
                (media_type, source_path, dest_path, now()))
    job_id = cur.lastrowid; conn.commit(); conn.close(); return job_id

def set_job_status(job_id, status, dest_path=None):
    conn = get_conn(); cur = conn.cursor()
    if dest_path is None: cur.execute("UPDATE prepare_jobs SET status=? WHERE id=?", (status, job_id))
    else: cur.execute("UPDATE prepare_jobs SET status=?, dest_path=? WHERE id=?", (status, dest_path, job_id))
    conn.commit(); conn.close()

def add_job_event(job_id, phase, message, percent=None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                (job_id, now(), phase, message, percent))
    cur.execute("DELETE FROM job_events WHERE job_id=? AND id NOT IN (SELECT id FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT 100)", (job_id, job_id))
    conn.commit(); conn.close()

def finish_job(job_id, success=True):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE prepare_jobs SET status=?, finished_at=? WHERE id=?", ("done" if success else "failed", now(), job_id))
    conn.commit(); conn.close()





def register_prepare_worker(job_id):
    ACTIVE_PREPARE_WORKERS.add(int(job_id))

def unregister_prepare_worker(job_id):
    ACTIVE_PREPARE_WORKERS.discard(int(job_id))

def reconcile_prepare_running_jobs(reason="Recovered stale prepare slot"):
    # In multi-worker gunicorn, in-memory active sets are process-local and cannot
    # safely identify active jobs across workers. Only recover clearly stale jobs.
    try:
        stale_min_age = max(300, int(str(os.environ.get("PREPAC_PREPARE_RECOVERY_MIN_AGE_SECONDS", "1800") or "1800")))
    except Exception:
        stale_min_age = 1800

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, started_at FROM prepare_jobs WHERE status='running'")
    rows = cur.fetchall()
    changed = 0
    now_dt = datetime.now()
    for row in rows:
        jid = int(row[0])
        started_at = None
        try:
            started_at = datetime.fromisoformat(str(row[1])) if row[1] else None
        except Exception:
            started_at = None
        if started_at:
            age_seconds = int((now_dt - started_at).total_seconds())
            if age_seconds < stale_min_age:
                continue
        if jid in ACTIVE_PREPARE_WORKERS or jid in ACTIVE_PREPARE_PROCS:
            continue
        cur.execute("UPDATE prepare_jobs SET status='failed', finished_at=? WHERE id=? AND status='running'", (now(), jid))
        if cur.rowcount:
            changed += 1
            cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                        (jid, now(), 'recovered', f"{reason} (stale > {stale_min_age}s)", None))
    conn.commit(); conn.close()
    return changed

def try_claim_prepare_slot(job_id, max_jobs):
    reconcile_prepare_running_jobs()
    conn = get_conn(); cur = conn.cursor()
    try:
        # Set a timeout to prevent hanging on database locks
        conn.execute("PRAGMA busy_timeout = 5000")  # 5 second timeout
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT COUNT(*) FROM prepare_jobs WHERE status='running'")
        running = int(cur.fetchone()[0] or 0)
        if running >= int(max_jobs):
            conn.rollback()
            conn.close()
            return False
        cur.execute("UPDATE prepare_jobs SET status='running' WHERE id=? AND status='queued'", (job_id,))
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
        logging.getLogger(__name__).warning(f"Failed to claim prepare slot for job {job_id}: {e}")
        return False

def list_jobs(limit=20):
    """Fetch jobs with recent events - optimized to avoid N+1 queries."""
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch job list
    cur.execute("SELECT * FROM prepare_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all recent events for these jobs in a single query
    placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT job_id, phase, message, percent, timestamp, id 
        FROM job_events 
        WHERE job_id IN ({placeholders})
        ORDER BY job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id (maintaining order - most recent first per job)
    events_by_job = {}
    for event in all_events:
        job_id = event["job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 10:  # Keep only last 10
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs and set latest phase/message/percent
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
        if j["events"]:
            j["phase"] = j["events"][0]["phase"]
            j["message"] = j["events"][0]["message"]
            j["percent"] = j["events"][0]["percent"]
        else:
            j["phase"] = ""
            j["message"] = ""
            j["percent"] = None
    
    return jobs


def list_jobs_by_status(statuses, limit=500):
    """Fetch jobs by status with recent events - optimized to avoid N+1 queries."""
    wanted = [str(s).strip().lower() for s in (statuses or []) if str(s).strip()]
    if not wanted:
        return []
    
    placeholders = ",".join("?" for _ in wanted)
    conn = get_conn(); cur = conn.cursor()
    
    # Fetch job list
    cur.execute(f"SELECT * FROM prepare_jobs WHERE lower(status) IN ({placeholders}) ORDER BY id DESC LIMIT ?", 
                tuple(wanted) + (int(limit),))
    jobs = [dict(r) for r in cur.fetchall()]
    
    if not jobs:
        conn.close()
        return jobs
    
    job_ids = [j["id"] for j in jobs]
    
    # Fetch all recent events for these jobs in a single query
    event_placeholders = ",".join("?" * len(job_ids))
    cur.execute(f"""
        SELECT job_id, phase, message, percent, timestamp, id 
        FROM job_events 
        WHERE job_id IN ({event_placeholders})
        ORDER BY job_id DESC, id DESC
    """, job_ids)
    
    all_events = cur.fetchall()
    conn.close()
    
    # Group events by job_id (maintaining order - most recent first per job)
    events_by_job = {}
    for event in all_events:
        job_id = event["job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 20:  # Keep last 20 for this function
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs and set latest phase/message/percent
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
        if j["events"]:
            j["phase"] = j["events"][0]["phase"]
            j["message"] = j["events"][0]["message"]
            j["percent"] = j["events"][0]["percent"]
        else:
            j["phase"] = ""
            j["message"] = ""
            j["percent"] = None
    
    return jobs


def interrupt_running_prepare_jobs(reason="Interrupted by container shutdown", recovery=False):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM prepare_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        phase = "recovered" if recovery else "shutdown"
        message = ("Recovered after restart: previous container exited during job execution"
                   if recovery else reason)
        cur.execute("UPDATE prepare_jobs SET status='failed', finished_at=?, message=? WHERE id=?",
                    (now(), message, row["id"]))
        cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (row["id"], now(), phase, message, row.get("percent")))
    conn.commit(); conn.close()
    return len(rows)


def get_prepare_job_status(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT status FROM prepare_jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return str(row[0]) if row else ""

def register_prepare_proc(job_id, proc):
    ACTIVE_PREPARE_PROCS[int(job_id)] = proc

def unregister_prepare_proc(job_id, proc=None):
    current = ACTIVE_PREPARE_PROCS.get(int(job_id))
    if proc is None or current is proc:
        ACTIVE_PREPARE_PROCS.pop(int(job_id), None)

def cancel_prepare_job(job_id, reason="Cancelled by user"):
    proc = ACTIVE_PREPARE_PROCS.get(int(job_id))
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE prepare_jobs SET status='cancelled', finished_at=? WHERE id=? AND status IN ('queued','running')", (now(), job_id))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    return changed
