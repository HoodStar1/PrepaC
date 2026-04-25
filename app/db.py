import sqlite3
from pathlib import Path
import logging
from app.file_locks import release_lock, try_acquire_lock

_LOG = logging.getLogger(__name__)

CONFIG_DIR = Path("/config")
DB_PATH = CONFIG_DIR / "prepac.db"
INTEGRITY_LOCK_PATH = CONFIG_DIR / "prepac_integrity.lock"
_INTEGRITY_CHECKED = False
_INTEGRITY_OK = None

DEFAULT_SETTINGS = {
    "config_root": "/config",
    "tv_root": "/media/tv",
    "movie_root": "/media/movies",
    "youtube_root": "/media/youtube",
    "dest_root": "/media/dest",
    "owner_user": "hoodstar",
    "owner_group": "users",
    "dest_mode": "775",
    "end_tag": "PrepaC",
    "prepare_max_concurrent_jobs": "1",
    "prepare_permissions_mode": "legacy_open",
    "packing_max_concurrent_jobs": "1",
    "max_name_len": "249",
    "win_path_warn": "240",
    "dry_run_prepare_delete": "true",
    "plex_url": "",
    "plex_token": "",
    "plex_tv_library": "",
    "plex_movie_library": "",
    "plex_youtube_library": "",
    "clean_dry_run": "true",
    "clean_use_recycle_bin": "true",
    "recycle_bin_root": "/media/dest/.prepac_recycle",
    "plex_client_id": "prepac-local-client",
    "plex_product_name": "PrepaC",
    "packing_watch_root": "/media/dest",
    "packing_output_root": "/media/dest/_packed",
    "packing_stability_delay": "30",
    "packing_delete_source_after_success": "true",
    "packing_password_prefix": "NZBCave_",
    "packing_password_length": "24",
    "packing_thumbnail_host": "freeimage",
    "packing_freeimage_api_key": "",
    "packing_header_encrypt": "true",
    "packing_auto_volume": "true",
    "packing_manual_volume_mb": "0",
    "packing_auto_par2": "true",
    "packing_manual_par2_percent": "0",
    "packing_par2_threads": "4",
    "packing_par2_memory_mb": "1024",
    "packing_par2_block_size": "0",
    "packing_name_length": "15",
    "packing_name_fixed_tag": "FS",
    "packing_name_fixed_pos": "4",
    "packing_ffmpeg_collage_width": "1500",
    "packing_ffmpeg_collage_height": "844",
    "posting_posted_root": "/media/dest/_posted",
    "posting_nzb_root": "/media/dest/_nzb",
    "posting_article_size": "768000",
    "posting_yenc_line_size": "8000",
    "posting_retries": "1",
    "posting_retry_delay": "0s",
    "posting_embed_password_in_nzb": "true",
    "posting_post_check": "false",
    "posting_comment": "",
    "posting_provider2_max_gb_when_busy": "25",
    "posting_provider1_enabled": "false",
    "posting_provider1_host": "",
    "posting_provider1_port": "563",
    "posting_provider1_ssl": "true",
    "posting_provider1_username": "",
    "posting_provider1_password": "",
    "posting_provider1_connections": "25",
    "posting_provider1_max_connections": "25",
    "posting_provider2_enabled": "false",
    "posting_provider2_host": "",
    "posting_provider2_port": "563",
    "posting_provider2_ssl": "true",
    "posting_provider2_username": "",
    "posting_provider2_password": "",
    "posting_provider2_connections": "25",
    "posting_provider2_max_connections": "25",
    "posting_providers_json": "[]",
    "auth_initialized": "false",
    "auth_username": "",
    "auth_password_hash": "",
    "auth_recovery_hash": "",
    "workflow_auto_chain_enabled": "false",
    "github_repo_owner": "HoodStar1",
    "github_repo_name": "PrepaC",
    "update_check_enabled": "true",
    "share_destinations_json": "[]",
    "share_import_root": "/media/dest/_share/imports",
    "share_auto_after_posting": "true",
    "share_request_timeout": "120"
}


def _normalized_sqlite_error_message(exc):
    return " ".join(str(exc or "").strip().lower().split())


def _is_ignorable_schema_migration_error(statement, exc):
    message = _normalized_sqlite_error_message(exc)
    normalized_statement = " ".join(str(statement or "").strip().lower().split())
    if normalized_statement.startswith("alter table ") and " add column " in normalized_statement:
        return "duplicate column name:" in message
    if normalized_statement.startswith("create index") or normalized_statement.startswith("create unique index"):
        return "already exists" in message
    return False


def _execute_schema_statement(cur, statement, *, operation="schema migration"):
    try:
        cur.execute(statement)
        return True
    except Exception as exc:
        if _is_ignorable_schema_migration_error(statement, exc):
            _LOG.info("Skipping already-applied %s: %s (%s)", operation, statement, exc)
            return False
        raise RuntimeError(f"Failed {operation}: {statement} ({exc})") from exc

def get_conn():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA optimize")
    except Exception:
        pass
    return conn

def check_db_integrity():
    """Run PRAGMA integrity_check and log a CRITICAL warning if the DB is corrupted."""
    global _INTEGRITY_CHECKED, _INTEGRITY_OK
    if _INTEGRITY_CHECKED:
        return bool(_INTEGRITY_OK)

    lock_handle = None
    try:
        # In multi-worker gunicorn, only one worker performs/logs integrity checks at startup.
        lock_handle = try_acquire_lock(INTEGRITY_LOCK_PATH)
        if lock_handle is None:
            _INTEGRITY_CHECKED = True
            _INTEGRITY_OK = True
            return True

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check")
        results = cur.fetchall()
        conn.close()
        if results and results[0][0] != "ok":
            issues = "; ".join(str(r[0]) for r in results[:5])
            _LOG.critical(
                "SQLite DB integrity check FAILED: %s. "
                "To recover, run on the host:\n"
                "  sqlite3 /config/prepac.db '.recover' > /tmp/recover.sql\n"
                "  sqlite3 /config/prepac_new.db < /tmp/recover.sql\n"
                "  cp /config/prepac.db /config/prepac.db.bak\n"
                "  mv /config/prepac_new.db /config/prepac.db",
                issues,
            )
            _INTEGRITY_CHECKED = True
            _INTEGRITY_OK = False
            return False
        _INTEGRITY_CHECKED = True
        _INTEGRITY_OK = True
        return True
    except Exception as exc:
        _LOG.critical("SQLite DB integrity check error: %s", exc)
        _INTEGRITY_CHECKED = True
        _INTEGRITY_OK = False
        return False
    finally:
        release_lock(lock_handle)

def db_is_corrupt():
    """Return True if the startup integrity check found corruption."""
    return _INTEGRITY_CHECKED and (_INTEGRITY_OK is False)


def run_db_integrity_check():
    """Run a fresh PRAGMA integrity_check and return a status dict."""
    from datetime import datetime
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check")
        results = cur.fetchall()
        conn.close()
        issues = [str(r[0]) for r in results]
        ok = len(issues) == 1 and issues[0] == "ok"
        return {
            "ok": ok,
            "issues": [] if ok else issues,
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
    except Exception as exc:
        from datetime import datetime
        return {
            "ok": False,
            "issues": [str(exc)],
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }


def run_db_reindex():
    """
    Rebuild all SQLite indexes in-place with REINDEX.
    Safe to run on a live database — does not modify table data.
    If successful, resets the cached integrity state so the UI banner clears.
    Returns a status dict compatible with run_db_integrity_check().
    """
    global _INTEGRITY_CHECKED, _INTEGRITY_OK
    from datetime import datetime
    try:
        conn = get_conn()
        conn.execute("REINDEX")
        conn.close()
    except Exception as exc:
        return {
            "ok": False,
            "issues": [f"REINDEX failed: {exc}"],
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
    # Reset cached state so check_db_integrity() reruns on next call
    _INTEGRITY_CHECKED = False
    _INTEGRITY_OK = None
    # Delete the lock file so the cross-worker lock is cleared for next check
    try:
        INTEGRITY_LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    # Re-run check to confirm the reindex fixed things
    check_db_integrity()
    return run_db_integrity_check()


def init_db():
    check_db_integrity()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    cur.execute("""CREATE TABLE IF NOT EXISTS prepare_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        media_type TEXT NOT NULL,
        status TEXT NOT NULL,
        source_path TEXT,
        dest_path TEXT,
        started_at TEXT NOT NULL,
        finished_at TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS prepared_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        media_type TEXT NOT NULL,
        source_path TEXT NOT NULL,
        source_rel TEXT,
        copied_files_json TEXT NOT NULL,
        dest_path TEXT NOT NULL,
        source_bytes INTEGER NOT NULL,
        dest_bytes INTEGER NOT NULL,
        detected_tags_json TEXT,
        chosen_bracket TEXT,
        end_tag TEXT,
        created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS job_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        phase TEXT NOT NULL,
        message TEXT NOT NULL,
        percent INTEGER
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS packing_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_path TEXT NOT NULL,
        job_name TEXT NOT NULL,
        output_root TEXT NOT NULL,
        output_files_root TEXT NOT NULL,
        size_bytes INTEGER DEFAULT 0,
        status TEXT NOT NULL,
        phase TEXT,
        percent INTEGER,
        archive_token TEXT,
        password TEXT,
        rar_volume_bytes INTEGER DEFAULT 0,
        rar_parts_estimate INTEGER DEFAULT 0,
        par2_percent INTEGER DEFAULT 0,
        message TEXT,
        collage_path TEXT,
        imgbox_url TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS packing_job_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        packing_job_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        phase TEXT NOT NULL,
        message TEXT NOT NULL,
        percent INTEGER
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS posting_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT NOT NULL,
        packed_root TEXT NOT NULL,
        output_files_root TEXT NOT NULL,
        template_path TEXT NOT NULL,
        posted_root TEXT,
        size_bytes INTEGER DEFAULT 0,
        status TEXT NOT NULL,
        phase TEXT,
        percent INTEGER,
        provider_used TEXT,
        header_value TEXT,
        password_value TEXT,
        from_header TEXT,
        groups_csv TEXT,
        nzb_path TEXT,
        nzb_rar_path TEXT,
        message TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS posting_job_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        posting_job_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        phase TEXT NOT NULL,
        message TEXT NOT NULL,
        percent INTEGER
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS clean_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        reason TEXT NOT NULL,
        media_type TEXT NOT NULL,
        target_path TEXT NOT NULL,
        target_kind TEXT NOT NULL,
        dry_run TEXT NOT NULL,
        success TEXT NOT NULL,
        size_bytes INTEGER NOT NULL,
        breakdown_json TEXT NOT NULL,
        details_json TEXT NOT NULL,
        message TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS plex_pins (
        id INTEGER PRIMARY KEY,
        code TEXT,
        client_id TEXT,
        created_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS imported_share_bundles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        release_name TEXT NOT NULL,
        nzb_rar_path TEXT NOT NULL,
        template_path TEXT NOT NULL,
        mediainfo_override_path TEXT,
        size_bytes INTEGER DEFAULT 0,
        matched_by TEXT DEFAULT '',
        match_score INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS share_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_type TEXT NOT NULL,
        source_ref_id TEXT,
        posting_job_id INTEGER,
        import_bundle_id INTEGER,
        job_name TEXT NOT NULL,
        release_name TEXT NOT NULL,
        nzb_rar_path TEXT NOT NULL,
        template_path TEXT NOT NULL,
        generated_nfo_path TEXT,
        generated_mediainfo_path TEXT,
        detected_type TEXT,
        resolution_tier TEXT,
        category_key TEXT,
        selected_category_id TEXT,
        selected_category_label TEXT,
        destination_id TEXT NOT NULL,
        destination_name TEXT,
        status TEXT NOT NULL,
        phase TEXT,
        percent INTEGER,
        nzb_hash TEXT,
        job_hash TEXT,
        message TEXT,
        remote_id TEXT,
        remote_guid TEXT,
        raw_response TEXT,
        retry_count INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS share_job_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        share_job_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        phase TEXT NOT NULL,
        message TEXT NOT NULL,
        percent INTEGER
    )""")
    conn.commit()

    for stmt in [
        "ALTER TABLE packing_jobs ADD COLUMN rar_size_bytes INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN rar_parts_actual INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN rar_time_seconds INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN par2_size_bytes INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN par2_time_seconds INTEGER",
        "ALTER TABLE imported_share_bundles ADD COLUMN matched_by TEXT DEFAULT ''",
        "ALTER TABLE imported_share_bundles ADD COLUMN match_score INTEGER DEFAULT 0",
    ]:
        _execute_schema_statement(cur, stmt, operation="schema migration")

    for k, v in DEFAULT_SETTINGS.items():
        cur.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v))
    cur.execute("UPDATE settings SET value='true' WHERE key='packing_delete_source_after_success' AND (value IS NULL OR value = '' OR value = 'false')")

    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_prepare_jobs_status_id ON prepare_jobs(status, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prepare_jobs_finished_at ON prepare_jobs(finished_at)",
        "CREATE INDEX IF NOT EXISTS idx_prepared_items_created_at ON prepared_items(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_prepared_items_source_path ON prepared_items(source_path)",
        "CREATE INDEX IF NOT EXISTS idx_job_events_job_id_id ON job_events(job_id, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_packing_jobs_status_id ON packing_jobs(status, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_packing_jobs_finished_at ON packing_jobs(finished_at)",
        "CREATE INDEX IF NOT EXISTS idx_packing_job_events_job_id_id ON packing_job_events(packing_job_id, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_posting_jobs_status_id ON posting_jobs(status, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_posting_jobs_finished_at ON posting_jobs(finished_at)",
        "CREATE INDEX IF NOT EXISTS idx_posting_job_events_job_id_id ON posting_job_events(posting_job_id, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_clean_actions_created_at ON clean_actions(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_clean_actions_reason_media ON clean_actions(reason, media_type, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_imported_share_bundles_created_at ON imported_share_bundles(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_share_jobs_status_id ON share_jobs(status, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_share_jobs_dest_job ON share_jobs(destination_id, job_name)",
        "CREATE INDEX IF NOT EXISTS idx_share_jobs_nzb_hash ON share_jobs(nzb_hash)",
        "CREATE INDEX IF NOT EXISTS idx_share_job_events_job_id_id ON share_job_events(share_job_id, id DESC)",
    ]:
        _execute_schema_statement(cur, stmt, operation="schema index creation")

    conn.commit()
    conn.close()

def load_settings():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM settings")
    data = DEFAULT_SETTINGS.copy()
    for row in cur.fetchall():
        data[row["key"]] = row["value"]
    conn.close()
    return data

def save_settings(data):
    conn = get_conn()
    cur = conn.cursor()
    for k, v in data.items():
        cur.execute(
            """INSERT INTO settings(key, value) VALUES(?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (k, str(v))
        )
    conn.commit()
    conn.close()

def save_pin(pin_id, code, client_id, created_at, status="pending"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO plex_pins(id, code, client_id, created_at, status)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET code=excluded.code, client_id=excluded.client_id, created_at=excluded.created_at, status=excluded.status""",
        (int(pin_id), code, client_id, created_at, status)
    )
    conn.commit()
    conn.close()

def update_pin_status(pin_id, status):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE plex_pins SET status=? WHERE id=?", (status, int(pin_id)))
    conn.commit()
    conn.close()
