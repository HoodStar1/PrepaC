from pathlib import Path

ALIAS_ROOTS = {
    "/media/youtube": ["/media/Youtube Downloads"],
    "/media/Youtube Downloads": ["/media/youtube"],
    "/media/movies": ["/media/Movies"],
    "/media/Movies": ["/media/movies"],
    "/media/tv": ["/media/TV Shows"],
    "/media/TV Shows": ["/media/tv"],
    "/media/dest": ["/media/TBP/Jobs"],
    "/media/TBP/Jobs": ["/media/dest"],
}

ALLOWED_ROOT_SETTING_KEYS = [
    "config_root",
    "tv_root",
    "movie_root",
    "youtube_root",
    "dest_root",
    "packing_watch_root",
    "packing_output_root",
    "posting_posted_root",
    "posting_nzb_root",
    "recycle_bin_root",
]

def _safe_resolve(p):
    try:
        return Path(p).resolve(strict=False)
    except Exception:
        return Path(p)

def build_allowed_roots(settings):
    roots = []
    for key in ALLOWED_ROOT_SETTING_KEYS:
        raw = str(settings.get(key, "") or "").strip()
        if not raw:
            continue
        resolved = _safe_resolve(raw)
        roots.append(resolved)
        for alias in ALIAS_ROOTS.get(str(raw), []):
            roots.append(_safe_resolve(alias))
        for alias in ALIAS_ROOTS.get(str(resolved), []):
            roots.append(_safe_resolve(alias))
    return list(dict.fromkeys(str(r) for r in roots))

def is_path_within_roots(path_value, allowed_roots):
    path_obj = _safe_resolve(path_value)
    for root in allowed_roots:
        root_obj = _safe_resolve(root)
        try:
            path_obj.relative_to(root_obj)
            return True
        except Exception:
            continue
    return False

def assert_path_within_roots(path_value, allowed_roots, label="path"):
    if not is_path_within_roots(path_value, allowed_roots):
        raise RuntimeError(f"{label} is outside allowed roots: {path_value}")

def assert_no_parent_traversal(path_value, label="path"):
    text = str(path_value or "")
    if ".." in Path(text).parts:
        raise RuntimeError(f"{label} contains parent traversal: {path_value}")
