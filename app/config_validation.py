from pathlib import Path

INTEGER_LIMITS = {
    "prepare_max_concurrent_jobs": (1, 32),
    "packing_max_concurrent_jobs": (1, 16),
    "packing_stability_delay": (0, 600),
    "packing_password_length": (8, 128),
    "packing_par2_threads": (1, 64),
    "packing_par2_memory_mb": (64, 32768),
    "packing_par2_block_size": (0, 1048576),
    "packing_name_length": (4, 64),
    "posting_article_size": (1024, 50 * 1024 * 1024),
    "posting_yenc_line_size": (64, 32768),
    "posting_retries": (0, 10),
    "posting_provider1_port": (1, 65535),
    "posting_provider2_port": (1, 65535),
    "posting_provider1_connections": (1, 128),
    "posting_provider1_max_connections": (1, 128),
    "posting_provider2_connections": (1, 128),
    "posting_provider2_max_connections": (1, 128),
    "posting_provider2_max_gb_when_busy": (0, 100000),
    "share_request_timeout": (5, 600),
}
BOOLEAN_KEYS = {
    "clean_dry_run", "clean_use_recycle_bin", "packing_delete_source_after_success", "packing_header_encrypt",
    "packing_auto_volume", "packing_auto_par2", "posting_embed_password_in_nzb", "posting_post_check",
    "posting_provider1_enabled", "posting_provider1_ssl", "posting_provider2_enabled", "posting_provider2_ssl",
    "workflow_auto_chain_enabled", "update_check_enabled", "auth_initialized"
}
PATH_KEYS = {
    "config_root", "tv_root", "movie_root", "youtube_root", "dest_root", "recycle_bin_root", "packing_watch_root",
    "packing_output_root", "posting_posted_root", "posting_nzb_root"
}

PREPARE_PERMISSION_MODES = {"legacy_open", "shared_safe", "owner_strict"}


def _to_bool_text(value) -> str:
    return "true" if str(value).strip().lower() in {"1", "true", "yes", "on"} else "false"


def _normalize_path(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return value
    return str(Path(value))


def normalize_settings(settings: dict) -> tuple[dict, list[str]]:
    cleaned = dict(settings)
    warnings = []

    username = str(cleaned.get("auth_username", "") or "").strip()
    password_hash = str(cleaned.get("auth_password_hash", "") or "").strip()
    if username and password_hash:
        if str(cleaned.get("auth_initialized", "")).strip().lower() != "true":
            warnings.append("Promoted auth_initialized to true because stored credentials already exist")
        cleaned["auth_initialized"] = "true"
    for key in BOOLEAN_KEYS:
        if key in cleaned:
            before = cleaned[key]
            cleaned[key] = _to_bool_text(before)
            if str(before) != cleaned[key]:
                warnings.append(f"Normalized boolean setting: {key}")
    for key, (minimum, maximum) in INTEGER_LIMITS.items():
        if key not in cleaned:
            continue
        raw = cleaned[key]
        try:
            value = int(str(raw).strip())
        except Exception:
            value = minimum
            warnings.append(f"Replaced invalid numeric setting: {key}")
        if value < minimum:
            value = minimum
            warnings.append(f"Clamped {key} to minimum {minimum}")
        elif value > maximum:
            value = maximum
            warnings.append(f"Clamped {key} to maximum {maximum}")
        cleaned[key] = str(value)
    for key in PATH_KEYS:
        if key in cleaned:
            cleaned[key] = _normalize_path(cleaned[key])

    mode = str(cleaned.get("prepare_permissions_mode", "legacy_open") or "legacy_open").strip().lower()
    if mode not in PREPARE_PERMISSION_MODES:
        mode = "legacy_open"
        warnings.append("Normalized prepare_permissions_mode to legacy_open")
    cleaned["prepare_permissions_mode"] = mode
    return cleaned, warnings
