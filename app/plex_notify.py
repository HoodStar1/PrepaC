import requests
from app.secret_utils import resolve_secret
from app.plex_clean_preview import get_library_key

def refresh_library(url, token, section_key):
    headers = {"X-Plex-Token": token, "Accept": "application/json"}
    r = requests.get(f"{url.rstrip('/')}/library/sections/{section_key}/refresh", headers=headers, timeout=30)
    r.raise_for_status()
    return True

def notify_after_clean(settings, candidates):
    url = (settings.get("plex_url") or "").strip()
    token = (resolve_secret("plex_token", settings) or settings.get("plex_token") or "").strip()
    if not url or not token:
        return {"ok": False, "message": "No Plex URL/token configured.", "refreshed": []}

    mapping = {
        "tv": settings.get("plex_tv_library", "").strip(),
        "movie": settings.get("plex_movie_library", "").strip(),
        "youtube": settings.get("plex_youtube_library", "").strip(),
    }

    refreshed = []
    seen = set()
    for c in candidates:
        media_type = c.get("media_type")
        lib_name = mapping.get(media_type, "")
        if not lib_name or lib_name in seen:
            continue
        key = get_library_key(url, token, lib_name)
        if key:
            try:
                refresh_library(url, token, key)
                refreshed.append({"media_type": media_type, "library": lib_name, "section_key": key})
                seen.add(lib_name)
            except Exception as e:
                refreshed.append({"media_type": media_type, "library": lib_name, "section_key": key, "error": str(e)})

    ok = any("error" not in r for r in refreshed)
    return {"ok": ok, "refreshed": refreshed}
