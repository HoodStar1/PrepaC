from datetime import datetime
from app.db import get_conn

ACTIVE_POSTING_PROCS = {}

def now():
    return datetime.now().isoformat(timespec="seconds")

def create_posting_job(job_name, packed_root, output_files_root, template_path, size_bytes=0):
    existing_id = get_existing_active_posting_job_id(packed_root)
    if existing_id:
        return existing_id
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO posting_jobs(job_name, packed_root, output_files_root, template_path, size_bytes, status, created_at) VALUES (?, ?, ?, ?, ?, 'queued', ?)",
        (job_name, packed_root, output_files_root, template_path, int(size_bytes or 0), now())
    )
    job_id = cur.lastrowid
    conn.commit(); conn.close()
    return job_id



def get_existing_active_posting_job_id(packed_root):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM posting_jobs WHERE packed_root=? AND status IN ('queued','running') ORDER BY id DESC LIMIT 1", (packed_root,))
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
    active_ids = {int(x) for x in (active_job_ids or set()) if str(x).isdigit()}
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, percent FROM posting_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    changed = 0
    for row in rows:
        job_id = int(row.get("id") or 0)
        if job_id in active_ids:
            continue
        cur.execute("UPDATE posting_jobs SET status='failed', finished_at=?, message=? WHERE id=?", (now(), reason, job_id))
        cur.execute("INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (job_id, now(), 'recovered', reason, row.get('percent')))
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
    try:
        return packed_finished_at > latest_post
    except Exception:
        return packed_finished_at != latest_post

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



def count_running_posting_jobs():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM posting_jobs WHERE status='running'")
    row = cur.fetchone()
    conn.close()
    return int(row[0] or 0)

def try_claim_posting_provider(job_id, provider_name):
    provider_name = str(provider_name or "").strip()
    if not provider_name:
        return False
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT COUNT(*) FROM posting_jobs WHERE status='running' AND provider_used=?", (provider_name,))
        running = int(cur.fetchone()[0] or 0)
        if running > 0:
            conn.rollback()
            conn.close()
            return False
        cur.execute(
            "UPDATE posting_jobs SET status='running', started_at=?, provider_used=? WHERE id=? AND status='queued'",
            (now(), provider_name, job_id),
        )
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


def list_posting_jobs_by_status(statuses, limit=500):
    wanted = [str(s).strip().lower() for s in (statuses or []) if str(s).strip()]
    if not wanted:
        return []
    placeholders = ",".join("?" for _ in wanted)
    conn = get_conn(); cur = conn.cursor()
    cur.execute(f"SELECT * FROM posting_jobs WHERE lower(status) IN ({placeholders}) ORDER BY id DESC LIMIT ?", tuple(wanted) + (int(limit),))
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
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE posting_jobs SET status='cancelled', finished_at=?, message=?, provider_used='' WHERE id=? AND status IN ('queued','running')", (now(), reason, job_id))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    return changed
