from datetime import datetime
from app.db import get_conn

def now():
    return datetime.now().isoformat(timespec="seconds")

def create_posting_job(job_name, packed_root, output_files_root, template_path, size_bytes=0):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO posting_jobs(job_name, packed_root, output_files_root, template_path, size_bytes, status, created_at) VALUES (?, ?, ?, ?, ?, 'queued', ?)",
        (job_name, packed_root, output_files_root, template_path, int(size_bytes or 0), now())
    )
    job_id = cur.lastrowid
    conn.commit(); conn.close()
    return job_id

def update_posting_job(job_id, **fields):
    if not fields:
        return
    conn = get_conn(); cur = conn.cursor()
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [job_id]
    cur.execute(f"UPDATE posting_jobs SET {cols} WHERE id=?", vals)
    conn.commit(); conn.close()

def add_posting_event(job_id, phase, message, percent=None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
        (job_id, now(), phase, message, percent)
    )
    cur.execute("DELETE FROM posting_job_events WHERE posting_job_id=? AND id NOT IN (SELECT id FROM posting_job_events WHERE posting_job_id=? ORDER BY id DESC LIMIT 100)", (job_id, job_id))
    cur.execute("UPDATE posting_jobs SET phase=?, percent=?, message=? WHERE id=?", (phase, percent, message, job_id))
    conn.commit(); conn.close()

def start_posting(job_id, provider_used=None):
    fields = {"status":"running", "started_at":now()}
    if provider_used:
        fields["provider_used"] = provider_used
    update_posting_job(job_id, **fields)

def finish_posting(job_id, success=True, message=""):
    update_posting_job(job_id, status="done" if success else "failed", finished_at=now(), message=message, percent=100 if success else None)

def list_posting_jobs(limit=200):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM posting_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    for j in jobs:
        cur.execute("SELECT phase,message,percent,timestamp FROM posting_job_events WHERE posting_job_id=? ORDER BY id DESC LIMIT 50", (j["id"],))
        j["events"] = [dict(r) for r in cur.fetchall()]
    conn.close(); return jobs

def list_posting_history(limit=1000):
    return list_posting_jobs(limit)

def has_successful_posting(job_name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM posting_jobs WHERE job_name=? AND status='done' LIMIT 1", (job_name,))
    row = cur.fetchone()
    conn.close(); return row is not None

def get_running_provider_names():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT provider_used FROM posting_jobs WHERE status='running' AND provider_used IS NOT NULL AND provider_used != ''")
    vals = [r[0] for r in cur.fetchall()]
    conn.close(); return vals


def interrupt_running_posting_jobs(reason="Interrupted by container shutdown", recovery=False):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM posting_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        phase = "recovered" if recovery else "shutdown"
        message = ("Recovered after restart: previous container exited during job execution"
                   if recovery else reason)
        cur.execute("UPDATE posting_jobs SET status='failed', finished_at=?, message=? WHERE id=?",
                    (now(), message, row["id"]))
        cur.execute("INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (row["id"], now(), phase, message, row.get("percent")))
    conn.commit(); conn.close()
    return len(rows)
