from pathlib import Path
import time

# Cache for resolved roots (TTL: 5 minutes)
_ROOT_CACHE = {"data": {}, "ttl": 300, "last_clean": time.time()}

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

def assert_no_symlinks_in_path(path_value, label="path"):
    """Verify path does not contain any symlinks to prevent traversal attacks."""
    current = Path(path_value)
    max_depth = 1000
    depth = 0
    seen = set()
    
    while str(current) != "/" and depth < max_depth:
        if str(current) in seen:
            raise RuntimeError(f"{label} contains symlink loop: {path_value}")
        seen.add(str(current))
        
        if current.is_symlink():
            raise RuntimeError(f"{label} contains symlink: {path_value}")
        
        try:
            current = current.parent
        except Exception:
            break
        depth += 1
    
    if depth >= max_depth:
        raise RuntimeError(f"{label} path too deep or contains loop: {path_value}")

def build_allowed_roots(settings):
    """Build list of allowed root paths with caching."""
    global _ROOT_CACHE
    
    # Clean expired cache periodically
    now = time.time()
    if now - _ROOT_CACHE["last_clean"] > 60:
        _ROOT_CACHE["data"].clear()
        _ROOT_CACHE["last_clean"] = now
    
    # Create cache key from settings
    cache_key = tuple(sorted((k, settings.get(k, "")) for k in ALLOWED_ROOT_SETTING_KEYS))
    if cache_key in _ROOT_CACHE["data"]:
        return _ROOT_CACHE["data"][cache_key]
    
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
    
    result = list(dict.fromkeys(str(r) for r in roots))
    _ROOT_CACHE["data"][cache_key] = result
    return result

def is_path_within_roots(path_value, allowed_roots):
    """Check if path is within allowed roots (no symlinks)."""
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
    """Verify path is within allowed roots and contains no symlinks."""
    # First check for symlinks (traversal protection)
    assert_no_symlinks_in_path(path_value, label)
    
    # Then check bounds
    if not is_path_within_roots(path_value, allowed_roots):
        raise RuntimeError(f"{label} is outside allowed roots: {path_value}")

def assert_no_parent_traversal(path_value, label="path"):
    text = str(path_value or "")
    if ".." in Path(text).parts:
        raise RuntimeError(f"{label} contains parent traversal: {path_value}")
