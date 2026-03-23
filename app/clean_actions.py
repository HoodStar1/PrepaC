import json
from datetime import datetime
from app.db import get_conn

def log_clean_action(reason, media_type, target_path, target_kind, dry_run, success, size_bytes, breakdown, details, message=""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO clean_actions(
        created_at, reason, media_type, target_path, target_kind, dry_run, success,
        size_bytes, breakdown_json, details_json, message
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
        datetime.now().isoformat(timespec="seconds"),
        reason,
        media_type,
        target_path,
        target_kind,
        "true" if dry_run else "false",
        "true" if success else "false",
        int(size_bytes),
        json.dumps(breakdown),
        json.dumps(details),
        message
    ))
    conn.commit()
    conn.close()

def list_clean_actions(limit=200):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM clean_actions ORDER BY id DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows
