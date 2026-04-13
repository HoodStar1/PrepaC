from datetime import datetime
from app.db import get_conn

ACTIVE_PACKING_PROCS = {}

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
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
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
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM packing_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    for j in jobs:
        cur.execute("SELECT phase,message,percent,timestamp FROM packing_job_events WHERE packing_job_id=? ORDER BY id DESC LIMIT 20", (j["id"],))
        j["events"] = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jobs


def list_packing_jobs_by_status(statuses, limit=500):
    wanted = [str(s).strip().lower() for s in (statuses or []) if str(s).strip()]
    if not wanted:
        return []
    placeholders = ",".join("?" for _ in wanted)
    conn = get_conn(); cur = conn.cursor()
    cur.execute(f"SELECT * FROM packing_jobs WHERE lower(status) IN ({placeholders}) ORDER BY id DESC LIMIT ?", tuple(wanted) + (int(limit),))
    jobs = [dict(r) for r in cur.fetchall()]
    for j in jobs:
        cur.execute("SELECT phase,message,percent,timestamp FROM packing_job_events WHERE packing_job_id=? ORDER BY id DESC LIMIT 20", (j["id"],))
        j["events"] = [dict(r) for r in cur.fetchall()]
    conn.close(); return jobs

def has_successful_packing(source_path):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM packing_jobs WHERE source_path=? AND status='done' LIMIT 1", (source_path,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def list_packing_history(limit=1000):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM packing_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    for j in jobs:
        cur.execute("SELECT phase,message,percent,timestamp FROM packing_job_events WHERE packing_job_id=? ORDER BY id DESC LIMIT 50", (j["id"],))
        j["events"] = [dict(r) for r in cur.fetchall()]
    conn.close()
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
