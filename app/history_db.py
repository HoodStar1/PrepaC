import json
from datetime import datetime
from app.db import get_conn

def save_prepared_item(media_type, source_path, source_rel, copied_files, dest_path, source_bytes, dest_bytes, detected_tags, chosen_bracket, end_tag):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""INSERT INTO prepared_items(
        media_type, source_path, source_rel, copied_files_json, dest_path,
        source_bytes, dest_bytes, detected_tags_json, chosen_bracket, end_tag, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    (media_type, source_path, source_rel, json.dumps(copied_files), dest_path,
     int(source_bytes), int(dest_bytes), json.dumps(detected_tags), chosen_bracket, end_tag,
     datetime.now().isoformat(timespec="seconds")))
    conn.commit(); conn.close()

def list_history(limit=100):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM prepared_items ORDER BY id DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close(); return rows


def delete_prepared_by_source_path(source_path: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM prepared_history WHERE source_path = ?", (source_path,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def delete_prepared_by_id(prepared_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM prepared_history WHERE id = ?", (prepared_id,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()
