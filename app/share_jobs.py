from app.db import get_conn
from app.timestamp_utils import local_now_iso

ACTIVE_SHARE_PROCS = {}


def now():
    return local_now_iso()


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


def create_share_job(return_created=False, **fields):
    conn = get_conn(); cur = conn.cursor()
    source_ref_id = str(fields.get('source_ref_id', '') or '')
    destination_id = str(fields.get('destination_id', '') or '')
    job_name = str(fields.get('job_name', '') or '')
    nzb_hash = str(fields.get('nzb_hash', '') or '')
    key = str(fields.get('idempotency_key') or f"share:{source_ref_id}:{destination_id}").strip() or None
    try:
        conn.execute("BEGIN IMMEDIATE")
        identities = (
            ("idempotency_key=?", (key,)),
            ("destination_id=? AND job_name=?", (destination_id, job_name)),
            ("destination_id=? AND nzb_hash=?", (destination_id, nzb_hash)),
            ("destination_id=? AND source_ref_id=?", (destination_id, source_ref_id)),
        )
        clauses = [clause for clause, values in identities if all(str(value or "").strip() for value in values)]
        params = [
            value
            for _clause, values in identities
            if all(str(value or "").strip() for value in values)
            for value in values
        ]
        row = None
        if clauses:
            row = cur.execute(
                "SELECT id FROM share_jobs WHERE (" + " OR ".join(clauses) + ") "
                "AND status IN ('queued','running','uploading','outcome_unknown') ORDER BY id DESC LIMIT 1",
                tuple(params),
            ).fetchone()
            if row:
                conn.commit()
                result = int(row[0])
                return (result, False) if return_created else result
        cur.execute(
            """INSERT INTO share_jobs(source_type, source_ref_id, posting_job_id, import_bundle_id, job_name, release_name, nzb_rar_path, template_path, detected_type, resolution_tier, category_key, selected_category_id, selected_category_label, destination_id, destination_name, status, nzb_hash, job_hash, created_at, idempotency_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)""",
            (
                fields.get('source_type',''), source_ref_id, fields.get('posting_job_id'), fields.get('import_bundle_id'),
                job_name, fields.get('release_name',''), fields.get('nzb_rar_path',''), fields.get('template_path',''),
                fields.get('detected_type',''), fields.get('resolution_tier',''), fields.get('category_key',''), fields.get('selected_category_id',''),
                fields.get('selected_category_label',''), destination_id, fields.get('destination_name',''),
                nzb_hash, fields.get('job_hash',''), now(), key,
            ),
        )
        job_id = cur.lastrowid
        conn.commit()
        return (job_id, True) if return_created else job_id
    finally:
        conn.close()


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
    cur.execute(
        "UPDATE share_jobs SET status=?, finished_at=?, message=?, percent=? "
        "WHERE id=? AND status IN ('running','uploading')",
        (("done" if success else "failed"), now(), message, (100 if success else None), int(job_id)),
    )
    conn.commit(); conn.close()


def mark_share_outcome_unknown(job_id, message):
    """Record an ambiguous result after the upload boundary was crossed."""
    safe_message = str(message or "Upload outcome is unknown; verify the destination before force retry")[:1000]
    conn = get_conn(); cur = conn.cursor()
    try:
        timestamp = now()
        cur.execute(
            "UPDATE share_jobs SET status='outcome_unknown', phase='outcome_unknown', "
            "finished_at=?, message=?, percent=NULL WHERE id=? AND status='uploading'",
            (timestamp, safe_message, int(job_id)),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO share_job_events(share_job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'outcome_unknown', ?, NULL)",
                (int(job_id), timestamp, safe_message),
            )
        conn.commit()
        return changed
    finally:
        conn.close()


def list_share_jobs(limit=500, per_job_event_limit=50):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM share_jobs ORDER BY id DESC LIMIT ?", (limit,))
    jobs = [dict(r) for r in cur.fetchall()]
    events_by_job = _load_share_job_events(
        cur,
        [j["id"] for j in jobs],
        per_job_limit=max(0, min(100, int(per_job_event_limit))),
    )
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
        cur.execute("SELECT 1 FROM share_jobs WHERE destination_id=? AND job_name=? AND status IN ('queued','running','uploading','outcome_unknown','done') LIMIT 1", (destination_id, job_name))
        checks['destination_job'] = cur.fetchone() is not None
    if destination_id and nzb_hash:
        cur.execute("SELECT 1 FROM share_jobs WHERE destination_id=? AND nzb_hash=? AND status IN ('queued','running','uploading','outcome_unknown','done') LIMIT 1", (destination_id, nzb_hash))
        checks['nzb_hash'] = cur.fetchone() is not None
    if destination_id and source_ref_id:
        cur.execute("SELECT 1 FROM share_jobs WHERE destination_id=? AND source_ref_id=? AND status IN ('queued','running','uploading','outcome_unknown','done') LIMIT 1", (destination_id, source_ref_id))
        checks['source_ref'] = cur.fetchone() is not None
    conn.close(); return checks


def increment_share_retry(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE share_jobs SET retry_count=COALESCE(retry_count,0)+1, status='queued', "
        "phase='queued', percent=0, message='Retry queued', started_at=NULL, finished_at=NULL "
        "WHERE id=? AND status='failed'",
        (int(job_id),),
    )
    changed = cur.rowcount > 0
    if changed:
        cur.execute(
            "INSERT INTO share_job_events(share_job_id, timestamp, phase, message, percent) VALUES (?, ?, 'queued', 'Retry queued', 0)",
            (int(job_id), now()),
        )
    conn.commit(); conn.close()
    return changed


def force_retry_share_outcome_unknown(job_id):
    """Explicit retry after an operator verifies the remote destination.

    A normal retry intentionally accepts only ``failed``.  Keeping this as a
    separate, conspicuously named transition prevents accidental replay of an
    upload whose acknowledgement was lost.
    """
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE share_jobs SET retry_count=COALESCE(retry_count,0)+1, status='queued', "
            "phase='queued', percent=0, message='Manual force retry queued after unknown remote outcome was acknowledged', "
            "started_at=NULL, finished_at=NULL WHERE id=? AND status='outcome_unknown'",
            (int(job_id),),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO share_job_events(share_job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'queued', 'Manual force retry queued after unknown remote outcome was acknowledged', 0)",
                (int(job_id), now()),
            )
        conn.commit()
        return changed
    finally:
        conn.close()


def claim_share_job(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE share_jobs SET status='running', started_at=?, finished_at=NULL "
        "WHERE id=? AND status='queued'",
        (now(), int(job_id)),
    )
    changed = cur.rowcount > 0
    conn.commit(); conn.close()
    return changed


def begin_share_upload(job_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE share_jobs SET status='uploading', phase='upload' WHERE id=? AND status='running'", (int(job_id),))
    changed = cur.rowcount > 0
    conn.commit(); conn.close()
    return changed


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
    cur.execute("SELECT id FROM share_jobs WHERE source_ref_id=? AND destination_id=? AND status IN ('queued','running','uploading','outcome_unknown') ORDER BY id DESC", (source_ref_id, destination_id))
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
    cur.execute("UPDATE share_jobs SET status='cancelled', finished_at=?, message=? WHERE id=? AND status IN ('queued','running')", (now(), reason, int(job_id)))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("INSERT INTO share_job_events(share_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)", (int(job_id), now(), 'cancelled', reason, None))
    conn.commit(); conn.close()
    return changed
