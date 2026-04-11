import os
from pathlib import Path

SECRET_SPECS = {
    "posting_provider1_password": {
        "env": "PREPAC_POSTING_PROVIDER1_PASSWORD",
        "file": "/run/secrets/prepac_posting_provider1_password",
    },
    "posting_provider2_password": {
        "env": "PREPAC_POSTING_PROVIDER2_PASSWORD",
        "file": "/run/secrets/prepac_posting_provider2_password",
    },
    "plex_token": {
        "env": "PREPAC_PLEX_TOKEN",
        "file": "/run/secrets/prepac_plex_token",
    },
    "packing_freeimage_api_key": {
        "env": "PREPAC_PACKING_FREEIMAGE_API_KEY",
        "file": "/run/secrets/prepac_packing_freeimage_api_key",
    },
    "auth_password_reset_token": {
        "env": "PREPAC_PASSWORD_RESET_TOKEN",
        "file": "/run/secrets/prepac_password_reset_token",
    },
    "share_destinations_json": {
        "env": "PREPAC_SHARE_DESTINATIONS_JSON",
        "file": "/run/secrets/prepac_share_destinations_json",
    },
    "posting_providers_json": {
        "env": "PREPAC_POSTING_PROVIDERS_JSON",
        "file": "/run/secrets/prepac_posting_providers_json",
    },
}

def resolve_secret(setting_key, settings=None):
    settings = settings or {}
    spec = SECRET_SPECS.get(setting_key, {})
    file_path = spec.get("file")
    env_name = spec.get("env")

    if file_path:
        p = Path(file_path)
        if p.exists():
            try:
                value = p.read_text(encoding="utf-8", errors="replace").strip()
                if value:
                    return value
            except Exception:
                pass

    if env_name:
        value = os.environ.get(env_name, "").strip()
        if value:
            return value

    return str(settings.get(setting_key, "") or "").strip()

def secret_source(setting_key, settings=None):
    settings = settings or {}
    spec = SECRET_SPECS.get(setting_key, {})
    file_path = spec.get("file")
    env_name = spec.get("env")

    if file_path and Path(file_path).exists():
        return "secret_file"
    if env_name and os.environ.get(env_name, "").strip():
        return "env_var"
    if str(settings.get(setting_key, "") or "").strip():
        return "saved_setting"
    return "unset"

def masked_secret_value(setting_key, settings=None):
    source = secret_source(setting_key, settings)
    if source == "unset":
        return ""
    if source == "secret_file":
        return "******** (from secret file)"
    if source == "env_var":
        return "******** (from env var)"
    return "******** (saved)"
