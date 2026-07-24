import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
import logging
from app.file_locks import release_lock, try_acquire_lock
from app.timestamp_utils import local_now_iso

_LOG = logging.getLogger(__name__)

CONFIG_DIR = Path(os.environ.get("PREPAC_CONFIG_DIR", "/config")).expanduser()
DB_PATH = CONFIG_DIR / "prepac.db"
INTEGRITY_LOCK_PATH = CONFIG_DIR / "prepac_integrity.lock"
MIGRATION_LOCK_PATH = CONFIG_DIR / "prepac_migration.lock"
SCHEMA_VERSION = 6
_INTEGRITY_CHECKED = False
_INTEGRITY_OK = None
_SQLITE_JOURNAL_MODES = {"DELETE", "WAL"}
_DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 15000
_MIN_SQLITE_BUSY_TIMEOUT_MS = 1000
_MAX_SQLITE_BUSY_TIMEOUT_MS = 60000

DEFAULT_SETTINGS = {
    "config_root": str(CONFIG_DIR),
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
    "posting_watch_root": "",
    "posting_nzb_root": "/media/dest/_nzb",
    "posting_article_size": "768000",
    "posting_yenc_line_size": "8000",
    "posting_retries": "1",
    "posting_retry_delay": "0s",
    "posting_connection_headroom": "2",
    "posting_provider_failure_cooldown_seconds": "300",
    "posting_provider_disconnect_drain_seconds": "60",
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
    "auth_session_epoch": "1",
    "auth_force_password_change": "false",
    "workflow_auto_chain_enabled": "false",
    "github_repo_owner": "HoodStar1",
    "github_repo_name": "PrepaC",
    "update_check_enabled": "true",
    "share_destinations_json": "[]",
    "share_watch_root": "",
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


def _quarantine_duplicate_active_jobs(cur):
    """Preserve but deactivate legacy duplicate jobs before unique indexes.

    Older/intermediate builds could race two in-memory workers into rows that
    represent the same active operation. Keeping the earliest row active and
    marking later rows failed avoids a migration outage without deleting any
    history or replaying a potentially destructive or external effect.
    """
    active_statuses = ("queued", "running", "finalizing", "uploading", "outcome_unknown")
    placeholders = ",".join("?" for _ in active_statuses)
    timestamp = local_now_iso()
    specs = (
        ("prepare_jobs", "job_events", "job_id", False, (("idempotency_key",), ("source_path",))),
        ("packing_jobs", "packing_job_events", "packing_job_id", True, (("idempotency_key",), ("source_path",))),
        ("posting_jobs", "posting_job_events", "posting_job_id", True, (("idempotency_key",), ("packed_root",))),
        (
            "share_jobs",
            "share_job_events",
            "share_job_id",
            True,
            (
                ("idempotency_key",),
                ("destination_id", "job_name"),
                ("destination_id", "nzb_hash"),
                ("destination_id", "source_ref_id"),
            ),
        ),
    )
    changed = 0
    for table, event_table, event_fk, has_message, identity_groups in specs:
        rows = cur.execute(
            f"SELECT * FROM {table} WHERE status IN ({placeholders}) ORDER BY id ASC",
            active_statuses,
        ).fetchall()
        seen = {}
        for row in rows:
            signatures = []
            for columns in identity_groups:
                values = tuple(str(row[column] or "").strip() for column in columns)
                if all(values):
                    signatures.append((columns, values))
            duplicate_of = next((seen[item] for item in signatures if item in seen), None)
            if duplicate_of is None:
                for signature in signatures:
                    seen[signature] = int(row["id"])
                continue
            message = (
                f"Migration recovery quarantined duplicate active job; retained job #{duplicate_of}. "
                "Review outputs before retrying."
            )
            message_clause = ", message=?" if has_message else ""
            params = [timestamp]
            if has_message:
                params.append(message)
            params.extend((int(row["id"]), str(row["status"])))
            cur.execute(
                f"UPDATE {table} SET status='failed', finished_at=?{message_clause} "
                "WHERE id=? AND status=?",
                tuple(params),
            )
            if cur.rowcount != 1:
                continue
            cur.execute(
                f"INSERT INTO {event_table}({event_fk}, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'migration_recovery', ?, NULL)",
                (int(row["id"]), timestamp, message),
            )
            changed += 1
    if changed:
        _LOG.warning("Quarantined %s duplicate active job row(s) during schema migration", changed)
    return changed

def _sqlite_busy_timeout_ms():
    raw_value = str(
        os.environ.get("PREPAC_SQLITE_BUSY_TIMEOUT_MS", _DEFAULT_SQLITE_BUSY_TIMEOUT_MS)
    ).strip()
    try:
        timeout_ms = int(raw_value, 10)
    except (TypeError, ValueError):
        timeout_ms = _DEFAULT_SQLITE_BUSY_TIMEOUT_MS
        _LOG.warning(
            "Invalid PREPAC_SQLITE_BUSY_TIMEOUT_MS value; using %sms",
            timeout_ms,
        )
    if not _MIN_SQLITE_BUSY_TIMEOUT_MS <= timeout_ms <= _MAX_SQLITE_BUSY_TIMEOUT_MS:
        fallback = min(
            _MAX_SQLITE_BUSY_TIMEOUT_MS,
            max(_MIN_SQLITE_BUSY_TIMEOUT_MS, timeout_ms),
        )
        _LOG.warning(
            "PREPAC_SQLITE_BUSY_TIMEOUT_MS must be between %s and %s; using %sms",
            _MIN_SQLITE_BUSY_TIMEOUT_MS,
            _MAX_SQLITE_BUSY_TIMEOUT_MS,
            fallback,
        )
        timeout_ms = fallback
    return timeout_ms


def _requested_journal_mode():
    requested = str(os.environ.get("PREPAC_SQLITE_JOURNAL_MODE", "DELETE")).strip().upper()
    if requested not in _SQLITE_JOURNAL_MODES:
        raise ValueError(
            "PREPAC_SQLITE_JOURNAL_MODE must be DELETE or WAL"
        )
    return requested


def _database_journal_mode(conn):
    row = conn.execute("PRAGMA journal_mode").fetchone()
    return str(row[0] if row else "").strip().upper()


def _configure_journal_mode(conn):
    requested = _requested_journal_mode()
    current = _database_journal_mode(conn)
    if current == requested:
        return current
    row = conn.execute(f"PRAGMA journal_mode={requested}").fetchone()
    applied = str(row[0] if row else "").strip().upper()
    if applied != requested:
        raise RuntimeError(
            f"SQLite refused journal mode {requested}; active mode is {applied or 'unknown'}"
        )
    _LOG.info("Changed SQLite journal mode from %s to %s", current or "unknown", applied)
    return applied


def _open_sqlite_connection():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(CONFIG_DIR, 0o700)
    except OSError:
        pass
    timeout_ms = _sqlite_busy_timeout_ms()
    conn = None
    try:
        conn = sqlite3.connect(
            DB_PATH,
            timeout=timeout_ms / 1000.0,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    except Exception:
        if conn is not None:
            conn.close()
        raise
    try:
        os.chmod(DB_PATH, 0o600)
    except OSError:
        pass
    return conn


def _configure_connection_pragmas(conn):
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")


def get_conn():
    conn = _open_sqlite_connection()
    try:
        _configure_connection_pragmas(conn)
    except Exception:
        conn.close()
        raise
    return conn


def _database_user_version(conn):
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0] if row else 0)


def _database_has_user_tables(conn):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1"
    ).fetchone()
    return bool(row)


def _integrity_ok(conn):
    rows = conn.execute("PRAGMA integrity_check").fetchall()
    return len(rows) == 1 and str(rows[0][0]).strip().lower() == "ok"


def _acquire_migration_lock():
    lock_path = CONFIG_DIR / "prepac_migration.lock"
    handle = try_acquire_lock(lock_path)
    if handle is not None:
        return handle
    for _ in range(300):
        time.sleep(0.1)
        handle = try_acquire_lock(lock_path)
        if handle is not None:
            return handle
    raise RuntimeError("Timed out waiting for the database migration lock")


def _files_identical(first, second):
    if first.stat().st_size != second.stat().st_size:
        return False
    with first.open("rb") as first_handle, second.open("rb") as second_handle:
        while True:
            first_chunk = first_handle.read(1024 * 1024)
            second_chunk = second_handle.read(1024 * 1024)
            if first_chunk != second_chunk:
                return False
            if not first_chunk:
                return True


def _create_migration_backup_if_needed():
    wal_path = Path(f"{DB_PATH}-wal")
    if (
        (not DB_PATH.exists() or DB_PATH.stat().st_size == 0)
        and wal_path.exists()
        and wal_path.stat().st_size > 0
    ):
        raise RuntimeError(
            "Refusing to initialize while a non-empty SQLite WAL exists without its database"
        )
    if not DB_PATH.exists() or DB_PATH.stat().st_size == 0:
        return None
    timeout_ms = _sqlite_busy_timeout_ms()
    source = _open_sqlite_connection()
    try:
        current_version = _database_user_version(source)
        if current_version > SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {current_version} is newer than supported "
                f"version {SCHEMA_VERSION}"
            )
        has_user_tables = _database_has_user_tables(source)
        current_journal = _database_journal_mode(source)
        requested_journal = _requested_journal_mode()
        needs_migration = current_version < SCHEMA_VERSION and has_user_tables
        needs_journal_change = current_journal != requested_journal and has_user_tables
        if not needs_migration and not needs_journal_change:
            return None
        if not _integrity_ok(source):
            raise RuntimeError("Refusing to change a database that failed integrity_check")
        backup_dir = CONFIG_DIR / "migration_backups"
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if needs_migration:
            backup_prefix = f"prepac-before-v{current_version}-to-v{SCHEMA_VERSION}"
            backup_reason = "migration"
        else:
            backup_prefix = (
                f"prepac-before-journal-{current_journal.lower()}-to-"
                f"{requested_journal.lower()}"
            )
            backup_reason = "journal-mode change"
        backup_stem = f"{backup_prefix}-{stamp}"
        backup_path = backup_dir / f"{backup_stem}.db"
        suffix = 1
        while backup_path.exists():
            backup_path = backup_dir / f"{backup_stem}-{suffix}.db"
            suffix += 1
        target = sqlite3.connect(backup_path, timeout=timeout_ms / 1000.0)
        try:
            target.execute(f"PRAGMA busy_timeout={timeout_ms}")
            source.backup(target)
            if not _integrity_ok(target):
                raise RuntimeError("Pre-change backup failed integrity_check")
        finally:
            target.close()
        for existing_backup in sorted(backup_dir.glob(f"{backup_prefix}-*.db")):
            if existing_backup == backup_path:
                continue
            try:
                if not _files_identical(existing_backup, backup_path):
                    continue
                backup_path.unlink()
                _LOG.warning(
                    "Reusing identical retained pre-%s SQLite backup: %s",
                    backup_reason,
                    existing_backup,
                )
                return existing_backup
            except OSError:
                continue
        try:
            os.chmod(backup_dir, 0o700)
            os.chmod(backup_path, 0o600)
        except OSError:
            pass
        _LOG.warning("Created retained pre-%s SQLite backup: %s", backup_reason, backup_path)
        return backup_path
    finally:
        source.close()

def check_db_integrity():
    """Run PRAGMA integrity_check and log a CRITICAL warning if the DB is corrupted."""
    global _INTEGRITY_CHECKED, _INTEGRITY_OK
    if _INTEGRITY_CHECKED:
        return bool(_INTEGRITY_OK)

    lock_handle = None
    conn = None
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
        if results and results[0][0] != "ok":
            issues = "; ".join(str(r[0]) for r in results[:5])
            _LOG.critical(
                "SQLite DB integrity check FAILED: %s. "
                "Stop PrepaC, retain an untouched copy of %s, then follow the documented "
                "SQLite recovery procedure for your platform.",
                issues, DB_PATH,
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
        if conn is not None:
            conn.close()
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


def _apply_schema_migrations(conn):
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
        provider_lock TEXT,
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
    cur.execute("""CREATE TABLE IF NOT EXISTS auth_rate_limits (
        rate_key TEXT PRIMARY KEY,
        failures_json TEXT NOT NULL DEFAULT '[]',
        locked_until REAL NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL DEFAULT 0
    )""")

    for stmt in [
        "ALTER TABLE packing_jobs ADD COLUMN rar_size_bytes INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN rar_parts_actual INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN rar_time_seconds INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN par2_size_bytes INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN par2_time_seconds INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN output_reset_claimed_at TEXT",
        "ALTER TABLE posting_jobs ADD COLUMN provider_lock TEXT",
        "ALTER TABLE imported_share_bundles ADD COLUMN matched_by TEXT DEFAULT ''",
        "ALTER TABLE imported_share_bundles ADD COLUMN match_score INTEGER DEFAULT 0",
        "ALTER TABLE prepare_jobs ADD COLUMN idempotency_key TEXT",
        "ALTER TABLE packing_jobs ADD COLUMN idempotency_key TEXT",
        "ALTER TABLE posting_jobs ADD COLUMN idempotency_key TEXT",
        "ALTER TABLE share_jobs ADD COLUMN idempotency_key TEXT",
    ]:
        _execute_schema_statement(cur, stmt, operation="schema migration")

    for k, v in DEFAULT_SETTINGS.items():
        cur.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v))

    _quarantine_duplicate_active_jobs(cur)

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
        "CREATE INDEX IF NOT EXISTS idx_posting_jobs_status_provider_lock ON posting_jobs(status, provider_lock)",
        "CREATE INDEX IF NOT EXISTS idx_posting_jobs_finished_at ON posting_jobs(finished_at)",
        "CREATE INDEX IF NOT EXISTS idx_posting_job_events_job_id_id ON posting_job_events(posting_job_id, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_clean_actions_created_at ON clean_actions(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_clean_actions_reason_media ON clean_actions(reason, media_type, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_imported_share_bundles_created_at ON imported_share_bundles(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_share_jobs_status_id ON share_jobs(status, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_share_jobs_dest_job ON share_jobs(destination_id, job_name)",
        "CREATE INDEX IF NOT EXISTS idx_share_jobs_nzb_hash ON share_jobs(nzb_hash)",
        "CREATE INDEX IF NOT EXISTS idx_share_job_events_job_id_id ON share_job_events(share_job_id, id DESC)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_prepare_jobs_active_idempotency ON prepare_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_packing_jobs_active_idempotency_v2 ON packing_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','finalizing')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_posting_jobs_active_idempotency_v2 ON posting_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','finalizing')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_share_jobs_active_idempotency_v2 ON share_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','uploading')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_prepare_jobs_active_idempotency_v3 ON prepare_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','finalizing','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_packing_jobs_active_idempotency_v3 ON packing_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','finalizing','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_posting_jobs_active_idempotency_v3 ON posting_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','finalizing','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_share_jobs_active_idempotency_v3 ON share_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued','running','uploading','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_prepare_jobs_active_source_v3 ON prepare_jobs(source_path) WHERE source_path != '' AND status IN ('queued','running','finalizing','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_packing_jobs_active_source_v3 ON packing_jobs(source_path) WHERE source_path != '' AND status IN ('queued','running','finalizing','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_posting_jobs_active_source_v3 ON posting_jobs(packed_root) WHERE packed_root != '' AND status IN ('queued','running','finalizing','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_share_jobs_active_destination_nzb_v3 ON share_jobs(destination_id, nzb_hash) WHERE destination_id != '' AND nzb_hash IS NOT NULL AND nzb_hash != '' AND status IN ('queued','running','uploading','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_share_jobs_active_destination_source_v3 ON share_jobs(destination_id, source_ref_id) WHERE destination_id != '' AND source_ref_id IS NOT NULL AND source_ref_id != '' AND status IN ('queued','running','uploading','outcome_unknown')",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_share_jobs_active_destination_name_v3 ON share_jobs(destination_id, job_name) WHERE destination_id != '' AND job_name != '' AND status IN ('queued','running','uploading','outcome_unknown')",
        "CREATE INDEX IF NOT EXISTS idx_auth_rate_limits_updated_at ON auth_rate_limits(updated_at)",
    ]:
        _execute_schema_statement(cur, stmt, operation="schema index creation")

    cur.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _init_db_locked():
    transition_conn = _open_sqlite_connection()
    try:
        _configure_journal_mode(transition_conn)
    finally:
        transition_conn.close()
    if not check_db_integrity():
        raise RuntimeError("Database failed integrity_check before migration")
    conn = get_conn()
    try:
        current_version = _database_user_version(conn)
        if current_version > SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {current_version} is newer than supported "
                f"version {SCHEMA_VERSION}"
            )
        conn.execute("BEGIN IMMEDIATE")
        _apply_schema_migrations(conn)
        if not _integrity_ok(conn):
            raise RuntimeError("Database failed integrity_check during migration")
        conn.commit()
        try:
            conn.execute("PRAGMA optimize")
        except Exception:
            pass
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize or migrate the database under a cross-process lock and backup."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    migration_lock = _acquire_migration_lock()
    backup_path = None
    try:
        backup_path = _create_migration_backup_if_needed()
        _init_db_locked()
    except Exception as exc:
        restore_hint = f" Restore from {backup_path}." if backup_path else ""
        raise RuntimeError(f"Database initialization/migration failed.{restore_hint} ({exc})") from exc
    finally:
        release_lock(migration_lock)

def load_settings():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT key, value FROM settings")
        data = DEFAULT_SETTINGS.copy()
        for row in cur.fetchall():
            data[row["key"]] = row["value"]
        return data
    finally:
        if conn is not None:
            conn.close()

def save_settings(data):
    """Backward-compatible bulk writer. New request paths should use patch helpers."""
    save_settings_patch(data)


def _upsert_settings(cur, data):
    for k, v in data.items():
        cur.execute(
            """INSERT INTO settings(key, value) VALUES(?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (str(k), str(v)),
        )


def save_settings_patch(data):
    """Atomically upsert only the supplied keys, avoiding stale-snapshot overwrites."""
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _upsert_settings(conn.cursor(), dict(data or {}))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


AUTH_SETTING_KEYS = {
    "auth_initialized", "auth_username", "auth_password_hash",
    "auth_recovery_hash", "auth_session_epoch", "auth_force_password_change",
}


def update_auth_settings_atomic(updates, *, expected=None, increment_session_epoch=True):
    """Compare and update authentication settings in one write transaction.

    Returns the resulting auth-setting snapshot, or ``None`` if an expected value
    changed before the transaction acquired the write lock.
    """
    patch = dict(updates or {})
    expected = dict(expected or {})
    unknown = (set(patch) | set(expected)) - AUTH_SETTING_KEYS
    if unknown:
        raise ValueError(f"Non-auth settings supplied to auth transaction: {sorted(unknown)}")
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            "SELECT key, value FROM settings WHERE key IN ({})".format(
                ",".join("?" for _ in AUTH_SETTING_KEYS)
            ),
            tuple(AUTH_SETTING_KEYS),
        ).fetchall()
        current = {key: str(DEFAULT_SETTINGS.get(key, "")) for key in AUTH_SETTING_KEYS}
        current.update({str(row["key"]): str(row["value"]) for row in rows})
        if any(current.get(key, "") != str(value) for key, value in expected.items()):
            conn.rollback()
            return None
        if increment_session_epoch:
            try:
                epoch = max(1, int(current.get("auth_session_epoch", "1") or "1")) + 1
            except Exception:
                epoch = 2
            patch["auth_session_epoch"] = str(epoch)
        _upsert_settings(conn.cursor(), patch)
        conn.commit()
        current.update({key: str(value) for key, value in patch.items()})
        return current
    except Exception:
        conn.rollback()
        raise
    finally:
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
