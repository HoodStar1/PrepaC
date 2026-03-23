from datetime import datetime
from app.db import get_conn

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

def list_jobs(limit=20):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM prepare_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    for j in jobs:
        cur.execute("SELECT phase, message, percent, timestamp FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT 10", (j["id"],))
        j["events"] = [dict(r) for r in cur.fetchall()]
        if j["events"]:
            j["phase"] = j["events"][0]["phase"]; j["message"] = j["events"][0]["message"]; j["percent"] = j["events"][0]["percent"]
        else:
            j["phase"] = ""; j["message"] = ""; j["percent"] = None
    conn.close(); return jobs


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
