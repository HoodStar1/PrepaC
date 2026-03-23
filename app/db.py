import sqlite3
from pathlib import Path

CONFIG_DIR = Path("/config")
DB_PATH = CONFIG_DIR / "prepac.db"

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
    "posting_randomizer_file": "/app/NameRandomizer.txt",
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
    "auth_initialized": "false",
    "auth_username": "",
    "auth_password_hash": "",
    "auth_recovery_hash": "",
    "workflow_auto_chain_enabled": "false"
}

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

def init_db():
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
    conn.commit()

    for stmt in [
        "ALTER TABLE packing_jobs ADD COLUMN rar_size_bytes INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN rar_parts_actual INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN rar_time_seconds INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN par2_size_bytes INTEGER",
        "ALTER TABLE packing_jobs ADD COLUMN par2_time_seconds INTEGER",
    ]:
        try:
            cur.execute(stmt)
        except Exception:
            pass

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
    ]:
        try:
            cur.execute(stmt)
        except Exception:
            pass

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
