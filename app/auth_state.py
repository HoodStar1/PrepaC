def credentials_present(settings: dict | None) -> bool:
    settings = settings or {}
    username = str(settings.get("auth_username", "") or "").strip()
    password_hash = str(settings.get("auth_password_hash", "") or "").strip()
    return bool(username and password_hash)


def auth_is_initialized(settings: dict | None) -> bool:
    settings = settings or {}
    if credentials_present(settings):
        return True
    return str(settings.get("auth_initialized", "false")).lower() == "true" and credentials_present(settings)
