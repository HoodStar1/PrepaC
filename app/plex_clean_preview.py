import re
import time
from app.posters import target_poster
from pathlib import Path
import requests
from app.secret_utils import resolve_secret

PLEX_GET_CACHE = {}
PLEX_GET_CACHE_TTL_SECONDS = 30

EXCLUDED_TOPS = {"user", "user0", "remotes", "disks"}

MEDIA_MAP = {
    "/media/tv": "TV Shows",
    "/media/TV Shows": "TV Shows",
    "/media/movies": "Movies",
    "/media/Movies": "Movies",
    "/media/youtube": "Youtube Downloads",
    "/media/Youtube Downloads": "Youtube Downloads",
    "/media/dest": "TBP/Jobs",
    "/media/TBP/Jobs": "TBP/Jobs",
}

def plex_get(url, token, path, params=None):
    headers = {"X-Plex-Token": token, "Accept": "application/json"}
    cache_key = (url.rstrip("/"), path, tuple(sorted((params or {}).items())))
    now = time.time()
    cached = PLEX_GET_CACHE.get(cache_key)
    if cached and (now - cached["ts"] <= PLEX_GET_CACHE_TTL_SECONDS):
        return cached["data"]
    r = requests.get(url.rstrip("/") + path, headers=headers, params=params or {}, timeout=5)
    r.raise_for_status()
    data = r.json()
    PLEX_GET_CACHE[cache_key] = {"ts": now, "data": data}
    if len(PLEX_GET_CACHE) > 128:
        oldest_key = min(PLEX_GET_CACHE.items(), key=lambda kv: kv[1]["ts"])[0]
        PLEX_GET_CACHE.pop(oldest_key, None)
    return data

def list_libraries(url, token):
    data = plex_get(url, token, "/library/sections")
    return data.get("MediaContainer", {}).get("Directory", []) or []

def get_library_key(url, token, title):
    for lib in list_libraries(url, token):
        if str(lib.get("title","")).strip().lower() == title.strip().lower():
            return str(lib.get("key"))
    return None

def fetch_library_items(url, token, key):
    start = 0
    size = 200
    items = []
    while True:
        data = plex_get(url, token, f"/library/sections/{key}/all", {"X-Plex-Container-Start": start, "X-Plex-Container-Size": size})
        mc = data.get("MediaContainer", {})
        batch = mc.get("Metadata", []) or []
        items.extend(batch)
        total = int(mc.get("totalSize", len(items)))
        if len(items) >= total or not batch:
            break
        start += size
    return items

def fetch_tv_episodes(url, token, key):
    start = 0
    size = 200
    items = []
    while True:
        data = plex_get(url, token, f"/library/sections/{key}/allLeaves", {"X-Plex-Container-Start": start, "X-Plex-Container-Size": size})
        mc = data.get("MediaContainer", {})
        batch = mc.get("Metadata", []) or []
        items.extend(batch)
        total = int(mc.get("totalSize", len(items)))
        if len(items) >= total or not batch:
            break
        start += size
    return items

def item_is_played(item):
    return bool(item.get("viewCount"))

def part_files(item):
    out = []
    for media in item.get("Media", []) or []:
        for part in media.get("Part", []) or []:
            f = part.get("file")
            if f:
                out.append(str(f))
    return out

def best_thumb(*paths):
    for p in paths:
        if p:
            return p
    return ""

def plex_image_url(url, token, thumb_path):
    if not thumb_path:
        return ""
    from urllib.parse import quote
    return f"/api/plex/image?path={quote(str(thumb_path), safe='')}&v=1"

def container_to_host_user_path(path_str):
    s = str(Path(path_str))
    for prefix, share in MEDIA_MAP.items():
        if s == prefix or s.startswith(prefix + "/"):
            rel = s[len(prefix):].lstrip("/")
            host = Path("/host_mnt/user") / share
            if rel:
                host = host / rel
            return host
    if s.startswith("/mnt/user/"):
        return Path("/host_mnt/user") / Path(s).relative_to("/mnt/user")
    if s.startswith("/host_mnt/user/"):
        return Path(s)
    return None

def canonical_target_key(path_str):
    host_user = container_to_host_user_path(path_str)
    if host_user:
        return str(host_user)
    return str(Path(path_str))

def path_size(p: Path):
    try:
        if p.is_file():
            return p.stat().st_size
    except Exception:
        return 0

    total = 0
    try:
        for child in p.rglob("*"):
            try:
                if child.is_file():
                    total += child.stat().st_size
            except Exception:
                pass
    except Exception:
        return 0
    return total

def media_root_storage_paths(path_str):
    host_user = container_to_host_user_path(path_str)
    out = []

    if host_user:
        try:
            rel = host_user.relative_to("/host_mnt/user")
            base = Path("/host_mnt")
            if base.exists():
                for top in base.iterdir():
                    if not top.is_dir():
                        continue
                    if top.name in EXCLUDED_TOPS:
                        continue
                    target = top / rel
                    if target.exists():
                        out.append(target)
        except Exception:
            pass

        if not out and host_user.exists():
            out.append(host_user)

    original = Path(path_str)
    if not out and original.exists():
        out.append(original)

    seen = set()
    dedup = []
    for p in out:
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            dedup.append(p)
    return dedup

def storage_breakdown(path_str):
    locations = []
    total = 0

    for p in media_root_storage_paths(path_str):
        size = path_size(p)
        total += size
        locations.append({"storage_path": str(p), "bytes": size})

    return {"total_bytes": total, "locations": locations}

def target_exists(path_str):
    paths = media_root_storage_paths(path_str)
    if paths:
        return any(p.exists() for p in paths)
    host_user = container_to_host_user_path(path_str)
    if host_user and host_user.exists():
        return True
    original = Path(path_str)
    return original.exists()

def nearest_parent_target(file_path, youtube_root="/media/youtube"):
    fp = Path(file_path)
    youtube_roots = [
        str(Path(youtube_root)),
        "/media/youtube",
        "/media/Youtube Downloads",
        "/mnt/user/youtube",
        "/mnt/user/Youtube Downloads",
        "/host_mnt/user/youtube",
        "/host_mnt/user/Youtube Downloads",
    ]
    rel = None
    for root_str in youtube_roots:
        try:
            rel = fp.relative_to(Path(root_str))
            break
        except Exception:
            continue

    if rel is None:
        return str(fp.parent if fp.parent.exists() else fp)

    # Safe rule:
    # /root/channel/video_folder/file.mp4 -> delete video_folder
    # /root/channel/file.mp4 -> delete only the file, never the whole channel folder
    if len(rel.parts) >= 3:
        return str(fp.parent)
    if len(rel.parts) == 2:
        return str(fp)
    if len(rel.parts) == 1:
        return str(fp)
    return str(fp.parent if fp.parent.exists() else fp)

def _contains_query(item, query):
    q = (query or "").strip().lower()
    if not q:
        return True
    title = str(item.get("title","")).lower()
    grand = str(item.get("grandparentTitle","")).lower()
    parent = str(item.get("parentTitle","")).lower()
    return q in title or q in grand or q in parent

def search_posters_for_prepare(settings, media_type, query):
    url = settings.get("plex_url","").strip()
    token = resolve_secret("plex_token", settings)
    if not url or not token:
        return []

    lib_map = {
        "tv": settings.get("plex_tv_library","").strip(),
        "movie": settings.get("plex_movie_library","").strip(),
        "youtube": settings.get("plex_youtube_library","").strip(),
    }
    lib_name = lib_map.get(media_type, "")
    if not lib_name:
        return []

    key = get_library_key(url, token, lib_name)
    if not key:
        return []

    items = fetch_library_items(url, token, key)
    out = []
    for item in items:
        if media_type == "tv" and item.get("type") not in ("show",):
            continue
        if media_type == "movie" and item.get("type") not in ("movie",):
            continue
        if not _contains_query(item, query):
            continue
        out.append({
            "name": item.get("title",""),
            "poster_url": plex_image_url(url, token, best_thumb(item.get("thumb"), item.get("art"), item.get("parentThumb"), item.get("grandparentThumb"))),
        })
    return out[:50]


def _is_season_like_name(name: str) -> bool:
    n = (name or "").strip().lower()
    if n in {"specials", "season 00", "season 0", "s00"}:
        return True
    if re.match(r"^season\s*\d+$", n):
        return True
    if re.match(r"^s\d{1,2}$", n):
        return True
    return False



def _season_like_dirs(show_path: str):
    p = Path(show_path)
    if not p.exists() or not p.is_dir():
        return []
    return [child for child in p.iterdir() if child.is_dir() and _is_season_like_name(child.name)]

def _season_context_details(target_path: str):
    target = Path(target_path)
    show_path = target.parent if target.parent else target
    season_dirs = _season_like_dirs(str(show_path))
    other_season_dirs = [p for p in season_dirs if str(p) != str(target)]
    return {
        "season_parent_show_path": str(show_path),
        "season_folder_names_in_show": [p.name for p in season_dirs],
        "other_season_folder_names_in_show": [p.name for p in other_season_dirs],
        "selected_season_is_only_season_folder": len(other_season_dirs) == 0,
        "will_also_remove_show_folder": len(other_season_dirs) == 0,
    }

def preview_clean(settings, history_items, filter_reason="both", filter_type="all"):
    url = settings.get("plex_url","").strip()
    token = resolve_secret("plex_token", settings)
    if not url or not token:
        return {"error": "Plex URL and token are required for Clean preview.", "results": []}

    results = []

    if filter_reason in ("prepared", "both"):
        for item in history_items:
            if filter_type != "all" and item["media_type"] != filter_type:
                continue
            target = item["source_path"]
            if not target_exists(target):
                continue
            sb = storage_breakdown(target)
            is_tv_season = item["media_type"] == "tv" and _is_season_like_name(Path(target).name)
            target_kind = "season_folder" if is_tv_season else "source_path"
            details = {"prepared_item_id": item["id"], "dest_path": item["dest_path"], "reason_flags": ["previously_prepared"]}
            if is_tv_season:
                details.update(_season_context_details(target))
            results.append({
                "media_type": item["media_type"],
                "reason": "previously_prepared",
                "title": Path(item["source_path"]).name,
                "target_path": target,
                "target_kind": target_kind,
                "size_bytes": sb["total_bytes"],
                "breakdown": sb["locations"],
                "details": details,
                "poster_url": target_poster(target, item["media_type"], target_kind),
            })

    if filter_reason in ("played", "both"):
        movie_lib = settings.get("plex_movie_library","").strip()
        if movie_lib and filter_type in ("all", "movie"):
            key = get_library_key(url, token, movie_lib)
            if key:
                seen = set()
                for item in fetch_library_items(url, token, key):
                    if not item_is_played(item):
                        continue
                    parts = part_files(item)
                    if not parts:
                        continue
                    movie_path = str(Path(parts[0]).parent)
                    if movie_path in seen:
                        continue
                    seen.add(movie_path)
                    if not target_exists(movie_path):
                        continue
                    sb = storage_breakdown(movie_path)
                    results.append({
                        "media_type": "movie",
                        "reason": "fully_played",
                        "title": item.get("title",""),
                        "target_path": movie_path,
                        "target_kind": "movie_folder",
                        "size_bytes": sb["total_bytes"],
                        "breakdown": sb["locations"],
                        "details": {"plex_files": parts, "reason_flags": ["fully_played"]},
                        "poster_url": target_poster(target, "youtube", "nearest_parent") or plex_image_url(url, token, best_thumb(item.get("thumb"), item.get("art"), item.get("parentThumb"), item.get("grandparentThumb"))),
                    })

        yt_lib = settings.get("plex_youtube_library","").strip()
        if yt_lib and filter_type in ("all", "youtube"):
            key = get_library_key(url, token, yt_lib)
            if key:
                seen = set()
                for item in fetch_library_items(url, token, key):
                    if not item_is_played(item):
                        continue
                    for pf in part_files(item):
                        target = nearest_parent_target(pf, settings.get("youtube_root","/media/youtube"))
                        if target in seen:
                            continue
                        seen.add(target)
                        if not target_exists(target):
                            continue
                        sb = storage_breakdown(target)
                        results.append({
                            "media_type": "youtube",
                            "reason": "fully_played",
                            "title": item.get("title",""),
                            "target_path": target,
                            "target_kind": "nearest_parent",
                            "size_bytes": sb["total_bytes"],
                            "breakdown": sb["locations"],
                            "details": {"plex_file": pf, "reason_flags": ["fully_played"]},
                            "poster_url": target_poster(target, "youtube", "nearest_parent") or plex_image_url(url, token, best_thumb(item.get("thumb"), item.get("art"), item.get("parentThumb"), item.get("grandparentThumb"))),
                        })

        tv_lib = settings.get("plex_tv_library","").strip()
        if tv_lib and filter_type in ("all", "tv"):
            key = get_library_key(url, token, tv_lib)
            if key:
                episodes = [i for i in fetch_tv_episodes(url, token, key) if (i.get("type") == "episode" or i.get("grandparentTitle"))]
                season_groups = {}
                for ep in episodes:
                    files = part_files(ep)
                    if not files:
                        continue
                    ep_file = Path(files[0])
                    season_path = str(ep_file.parent)
                    show_path = str(ep_file.parent.parent) if ep_file.parent.parent else str(ep_file.parent)
                    g = season_groups.setdefault(
                        season_path,
                        {
                            "show_path": show_path,
                            "show_title": str(ep.get("grandparentTitle","")).strip() or Path(show_path).name,
                            "season_name": ep_file.parent.name,
                            "played": 0,
                            "total": 0,
                            "sample_files": [],
                            "season_thumb": best_thumb(ep.get("parentThumb"), ep.get("grandparentThumb"), ep.get("thumb"), ep.get("art")),
                            "show_thumb": best_thumb(ep.get("grandparentThumb"), ep.get("thumb"), ep.get("art"), ep.get("parentThumb")),
                        },
                    )
                    g["total"] += 1
                    if item_is_played(ep):
                        g["played"] += 1
                    if len(g["sample_files"]) < 5:
                        g["sample_files"].append(str(ep_file))


                show_aggregate = {}
                for season_path, g in season_groups.items():
                    show_aggregate.setdefault(
                        g["show_path"],
                        {
                            "show_title": g["show_title"],
                            "season_count": 0,
                            "qualified_seasons": 0,
                            "show_thumb": g["show_thumb"],
                            "season_rows": [],
                        },
                    )
                    sg = show_aggregate[g["show_path"]]
                    sg["season_count"] += 1
                    is_qualified = g["played"] == g["total"] and g["total"] > 0
                    if is_qualified:
                        sg["qualified_seasons"] += 1
                    sg["season_rows"].append(
                        {
                            "season_path": season_path,
                            "season_name": g["season_name"],
                            "episodes_played": g["played"],
                            "episodes_total": g["total"],
                            "sample_files": g["sample_files"],
                            "season_thumb": g["season_thumb"],
                            "qualified": is_qualified,
                        }
                    )

                for show_path, sg in show_aggregate.items():
                    # If all seasons qualify, only show the show card.
                    if sg["season_count"] > 0 and sg["qualified_seasons"] == sg["season_count"]:
                        sb = storage_breakdown(show_path)
                        results.append(
                            {
                                "media_type": "tv",
                                "reason": "fully_played",
                                "title": sg["show_title"],
                                "target_path": show_path,
                                "target_kind": "show_folder",
                                "size_bytes": sb["total_bytes"],
                                "breakdown": sb["locations"],
                                "details": {
                                    "seasons": sg["season_count"],
                                    "seasons_removed": sg["season_count"],
                                    "reason_flags": ["fully_played"],
                                    "show_path": show_path,
                                    "will_also_remove_show_folder": True,
                                },
                                "poster_url": target_poster(show_path, "tv", "show_folder") or plex_image_url(url, token, sg["show_thumb"]),
                            }
                        )
                    else:
                        # Otherwise only show the individual qualified seasons.
                        for row in sg["season_rows"]:
                            if not row["qualified"]:
                                continue
                            sb = storage_breakdown(row["season_path"])
                            results.append(
                                {
                                    "media_type": "tv",
                                    "reason": "fully_played",
                                    "title": f"{sg['show_title']} / {row['season_name']}",
                                    "target_path": row["season_path"],
                                    "target_kind": "season_folder",
                                    "size_bytes": sb["total_bytes"],
                                    "breakdown": sb["locations"],
                                    "details": {
                                        "episodes_played": row["episodes_played"],
                                        "episodes_total": row["episodes_total"],
                                        "sample_files": row["sample_files"],
                                        "reason_flags": ["fully_played"],
                                        **_season_context_details(row["season_path"]),
                                    },
                                    "poster_url": plex_image_url(url, token, row["season_thumb"]),
                                }
                            )

    merged = {}
    for r in results:
        key = (canonical_target_key(r["target_path"]), r["media_type"])
        if key not in merged:
            merged[key] = r
        else:
            existing = merged[key]
            if existing["reason"] != r["reason"]:
                existing["reason"] = "both"
                existing["details"]["also"] = r["details"]
                flags = set(existing["details"].get("reason_flags", [])) | set(r["details"].get("reason_flags", []))
                existing["details"]["reason_flags"] = sorted(flags)
            existing["size_bytes"] = max(existing.get("size_bytes", 0), r.get("size_bytes", 0))
            if len(r.get("breakdown", [])) > len(existing.get("breakdown", [])):
                existing["breakdown"] = r["breakdown"]
            if not existing.get("poster_url") and r.get("poster_url"):
                existing["poster_url"] = r["poster_url"]

    out = list(merged.values())
    out.sort(key=lambda x: (x["media_type"], x["title"].lower()))
    return {"error": None, "results": out}
