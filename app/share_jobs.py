from datetime import datetime
from app.db import get_conn

ACTIVE_SHARE_PROCS = {}


def now():
    return datetime.now().isoformat(timespec="seconds")


def _load_share_job_events(cur, job_ids, per_job_limit=50):
    normalized_job_ids = []
    for job_id in job_ids or []:
        try:
            normalized_job_ids.append(int(job_id))
        except Exception:
            continue
    normalized_job_ids = list(dict.fromkeys(normalized_job_ids))
    if not normalized_job_ids:
        return {}

    placeholders = ",".join("?" for _ in normalized_job_ids)
    cur.execute(
        f"SELECT share_job_id, phase, message, percent, timestamp FROM share_job_events WHERE share_job_id IN ({placeholders}) ORDER BY share_job_id ASC, id DESC",
        tuple(normalized_job_ids),
    )
    grouped = {job_id: [] for job_id in normalized_job_ids}
    for row in cur.fetchall():
        job_id = int(row["share_job_id"])
        bucket = grouped.setdefault(job_id, [])
        if len(bucket) >= per_job_limit:
            continue
        bucket.append(
            {
                "phase": row["phase"],
                "message": row["message"],
                "percent": row["percent"],
                "timestamp": row["timestamp"],
            }
        )
    return grouped


def create_imported_share_bundle(release_name, nzb_rar_path, template_path, mediainfo_override_path="", size_bytes=0, matched_by="", match_score=0):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO imported_share_bundles(release_name, nzb_rar_path, template_path, mediainfo_override_path, size_bytes, matched_by, match_score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (release_name, nzb_rar_path, template_path, mediainfo_override_path or "", int(size_bytes or 0), str(matched_by or ""), int(match_score or 0), now())
    )
    bundle_id = cur.lastrowid
    conn.commit(); conn.close()
    return bundle_id


def list_imported_share_bundles(limit=500):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM imported_share_bundles ORDER BY id DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close(); return rows


def get_imported_share_bundle(bundle_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM imported_share_bundles WHERE id=?", (int(bundle_id),))
    row = cur.fetchone()
    conn.close(); return dict(row) if row else None


def create_share_job(**fields):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """INSERT INTO share_jobs(source_type, source_ref_id, posting_job_id, import_bundle_id, job_name, release_name, nzb_rar_path, template_path, detected_type, resolution_tier, category_key, selected_category_id, selected_category_label, destination_id, destination_name, status, nzb_hash, job_hash, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)""",
        (
            fields.get('source_type',''), fields.get('source_ref_id',''), fields.get('posting_job_id'), fields.get('import_bundle_id'),
            fields.get('job_name',''), fields.get('release_name',''), fields.get('nzb_rar_path',''), fields.get('template_path',''),
            fields.get('detected_type',''), fields.get('resolution_tier',''), fields.get('category_key',''), fields.get('selected_category_id',''),
            fields.get('selected_category_label',''), fields.get('destination_id',''), fields.get('destination_name',''),
            fields.get('nzb_hash',''), fields.get('job_hash',''), now()
        )
    )
    job_id = cur.lastrowid
    conn.commit(); conn.close(); return job_id


def update_share_job(job_id, **fields):
    if not fields:
        return
    conn = get_conn(); cur = conn.cursor()
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [int(job_id)]
    cur.execute(f"UPDATE share_jobs SET {cols} WHERE id=?", vals)
    conn.commit(); conn.close()


def add_share_event(job_id, phase, message, percent=None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO share_job_events(share_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (int(job_id), now(), phase, message, percent))
    cur.execute("DELETE FROM share_job_events WHERE share_job_id=? AND id NOT IN (SELECT id FROM share_job_events WHERE share_job_id=? ORDER BY id DESC LIMIT 100)", (int(job_id), int(job_id)))
    cur.execute("UPDATE share_jobs SET phase=?, percent=?, message=? WHERE id=?", (phase, percent, message, int(job_id)))
    conn.commit(); conn.close()


def finish_share(job_id, success=True, message=""):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT status FROM share_jobs WHERE id=?", (int(job_id),))
    row = cur.fetchone()
    if row and str(row[0] or "").lower() == "cancelled":
        conn.close()
        return
    cur.execute("UPDATE share_jobs SET status=?, finished_at=?, message=?, percent=? WHERE id=?", (("done" if success else "failed"), now(), message, (100 if success else None), int(job_id)))
    conn.commit(); conn.close()


def list_share_jobs(limit=500):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM share_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    events_by_job = _load_share_job_events(cur, [j["id"] for j in jobs])
    for j in jobs:
        j["events"] = events_by_job.get(int(j["id"]), [])
    conn.close(); return jobs


def list_share_history(limit=5000):
    return list_share_jobs(limit)


def count_existing_share_duplicates(destination_id, job_name, nzb_hash, source_ref_id=""):
    conn = get_conn(); cur = conn.cursor()
    checks = {
        'destination_job': False,
        'nzb_hash': False,
        'source_ref': False,
    }
    if destination_id and job_name:
        cur.execute("SELECT 1 FROM share_jobs WHERE destination_id=? AND job_name=? AND status IN ('queued','running','done') LIMIT 1", (destination_id, job_name))
        checks['destination_job'] = cur.fetchone() is not None
    if nzb_hash:
        cur.execute("SELECT 1 FROM share_jobs WHERE nzb_hash=? AND status IN ('queued','running','done') LIMIT 1", (nzb_hash,))
        checks['nzb_hash'] = cur.fetchone() is not None
    if destination_id and source_ref_id:
        cur.execute("SELECT 1 FROM share_jobs WHERE destination_id=? AND source_ref_id=? AND status IN ('queued','running','done') LIMIT 1", (destination_id, source_ref_id))
        checks['source_ref'] = cur.fetchone() is not None
    conn.close(); return checks


def increment_share_retry(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE share_jobs SET retry_count=COALESCE(retry_count,0)+1, status='queued', started_at=NULL, finished_at=NULL WHERE id=?", (int(job_id),))
    conn.commit(); conn.close()


def get_share_job(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM share_jobs WHERE id=?", (int(job_id),))
    row = cur.fetchone()
    if not row:
        conn.close(); return None
    job = dict(row)
    events_by_job = _load_share_job_events(cur, [int(job_id)])
    job["events"] = events_by_job.get(int(job_id), [])
    conn.close(); return job


def get_existing_active_share_job_ids(source_ref_id, destination_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id FROM share_jobs WHERE source_ref_id=? AND destination_id=? AND status IN ('queued','running') ORDER BY id DESC", (source_ref_id, destination_id))
    rows = [int(r[0]) for r in cur.fetchall()]
    conn.close(); return rows


def get_share_job_status(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT status FROM share_jobs WHERE id=?", (int(job_id),))
    row = cur.fetchone()
    conn.close()
    return str(row[0]) if row else ""


def cancel_share_job(job_id, reason="Cancelled by user"):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE share_jobs SET status='cancelled', finished_at=?, message=? WHERE id=? AND status IN ('queued','running','failed')", (now(), reason, int(job_id)))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO share_job_events(share_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (int(job_id), now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    return changed
