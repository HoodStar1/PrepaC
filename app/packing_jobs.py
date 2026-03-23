from datetime import datetime
from app.db import get_conn

def now():
    return datetime.now().isoformat(timespec="seconds")

def create_packing_job(source_path, job_name, output_root, output_files_root):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO packing_jobs(source_path, job_name, output_root, output_files_root, status, created_at) VALUES (?, ?, ?, ?, 'queued', ?)",
        (source_path, job_name, output_root, output_files_root, now())
    )
    job_id = cur.lastrowid
    conn.commit(); conn.close()
    return job_id

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
