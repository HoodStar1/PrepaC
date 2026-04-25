import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode

import requests

from app.db import load_settings
from app.posting_jobs import list_posting_history
from app.secret_utils import resolve_secret
from app.web_security import normalize_service_base_url
from app.share_jobs import (
    add_share_event,
    create_imported_share_bundle,
    create_share_job,
    count_existing_share_duplicates,
    finish_share,
    get_existing_active_share_job_ids,
    get_share_job,
    get_share_job_status,
    list_imported_share_bundles,
    list_share_history,
    update_share_job,
)

ACTIVE_SHARE_JOB_IDS = set()

STANDARD_CATEGORY_MAP = {
    "movie_sd": ("2030", "Movies SD"),
    "movie_hd": ("2040", "Movies HD"),
    "movie_uhd": ("2045", "Movies UHD"),
    "tv_sd": ("5030", "TV SD"),
    "tv_hd": ("5040", "TV HD"),
    "tv_uhd": ("5045", "TV UHD"),
}
CATEGORY_KEY_OPTIONS = [{"value": k, "label": v[1]} for k, v in STANDARD_CATEGORY_MAP.items()]

YEAR_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
SEASON_EP_RE = re.compile(r"\bS(\d{1,2})(?:E(\d{1,3}))?\b", re.I)
RES_RE = re.compile(r"\b(2160p|1080p|720p|480p)\b", re.I)
MOVIE_HINTS = re.compile(r"\b(BluRay|WEB[- .]?DL|WEBRip|Remux|DVDRip|HDRip)\b", re.I)


def _now_settings():
    return load_settings()


def share_auto_after_posting_enabled(settings=None):
    settings = settings or _now_settings()
    return str(settings.get("share_auto_after_posting", "true")).lower() == "true"


def get_share_destinations(settings=None):
    settings = settings or _now_settings()
    raw = resolve_secret("share_destinations_json", settings)
    try:
        data = json.loads(raw or "[]")
        if isinstance(data, list):
            cleaned = []
            for idx, item in enumerate(data, start=1):
                if not isinstance(item, dict):
                    continue
                entry = dict(item)
                entry.setdefault("id", entry.get("name") or f"destination{idx}")
                entry.setdefault("name", entry["id"])
                entry.setdefault("enabled", True)
                entry.setdefault("mode", "both")
                entry.setdefault("include_nfo", True)
                entry.setdefault("include_mediainfo", True)
                entry.setdefault("includemeta", True)
                entry.setdefault("api_key", "")
                entry.setdefault("categories_cache", [])
                try:
                    entry["base_url"] = normalize_service_base_url(entry.get("base_url", ""))
                    entry.pop("base_url_error", None)
                except Exception as exc:
                    entry["base_url"] = str(entry.get("base_url", "") or "").strip()
                    entry["base_url_error"] = str(exc)
                overrides = entry.get("category_overrides") if isinstance(entry.get("category_overrides"), dict) else {}
                normalized_overrides = {}
                for key in STANDARD_CATEGORY_MAP.keys():
                    value = str(overrides.get(key, "") or "").strip()
                    if value:
                        normalized_overrides[key] = value
                entry["category_overrides"] = normalized_overrides
                cleaned.append(entry)
            return cleaned
    except Exception:
        return []
    return []


def save_share_destinations(destinations, settings=None):
    from app.db import save_settings
    current = dict(settings or _now_settings())
    current["share_destinations_json"] = json.dumps(destinations, ensure_ascii=False, indent=2)
    save_settings(current)


def public_share_destinations(settings=None):
    public = []
    for destination in get_share_destinations(settings):
        entry = {
            "id": destination.get("id", ""),
            "name": destination.get("name", ""),
            "enabled": bool(destination.get("enabled", True)),
            "mode": destination.get("mode", "manual"),
            "include_nfo": bool(destination.get("include_nfo", True)),
            "include_mediainfo": bool(destination.get("include_mediainfo", True)),
            "includemeta": bool(destination.get("includemeta", True)),
            "basic_auth": bool(destination.get("basic_auth", False)),
            "categories_cache": destination.get("categories_cache") or [],
            "category_overrides": destination.get("category_overrides") or {},
            "base_url_error": str(destination.get("base_url_error", "") or ""),
        }
        public.append(entry)
    return public


def get_hidden_share_candidate_ids(settings=None):
    settings = settings or _now_settings()
    raw = settings.get("share_hidden_candidate_ids_json", "[]")
    try:
        data = json.loads(raw or "[]")
        if isinstance(data, list):
            return {str(x) for x in data if str(x).strip()}
    except Exception:
        pass
    return set()


def save_hidden_share_candidate_ids(hidden_ids, settings=None):
    from app.db import save_settings
    current = dict(settings or _now_settings())
    current["share_hidden_candidate_ids_json"] = json.dumps(sorted({str(x) for x in hidden_ids if str(x).strip()}), ensure_ascii=False)
    save_settings(current)


def unhide_share_candidate_ids(candidate_ids, settings=None):
    hidden = get_hidden_share_candidate_ids(settings)
    hidden -= {str(x) for x in candidate_ids if str(x).strip()}
    save_hidden_share_candidate_ids(hidden, settings)


def remove_share_candidate(candidate_id, settings=None):
    hidden = get_hidden_share_candidate_ids(settings)
    hidden.add(str(candidate_id))
    save_hidden_share_candidate_ids(hidden, settings)
    return {"candidate_id": str(candidate_id), "removed": True}


def _pair_key_from_filename(filename):
    name = Path(str(filename or '')).name
    lowered = name.lower()
    for ext in ('.part.rar', '.rar', '.nzb', '.txt', '.xml', '.nfo'):
        if lowered.endswith(ext):
            name = name[: -len(ext)]
            lowered = name.lower()
            break
    name = re.sub(r'(?i)(?:[_\- ]template)$', '', name).strip()
    name = re.sub(r'[^a-z0-9]+', '', name.lower())
    return name


def _normalize_pair_string(value):
    return re.sub(r'[^a-z0-9]+', '', str(value or '').lower())


def _template_release_title(text):
    lines = [line.strip() for line in str(text or '').splitlines() if line.strip()]
    for line in lines:
        plain = re.sub(r'\[[^\]]+\]', '', line).strip()
        if not plain:
            continue
        if 'tag_here' in plain.lower():
            continue
        if any(tok in plain.lower() for tok in ('header:', 'group:', 'size:', 'password:')):
            continue
        release_hint = re.search(r'\[(?:web|blu|aac|eac3|ddp|x26|h26|2160p|1080p|720p)', line, re.I)
        episode_hint = re.search(r'\bS\d{1,2}(?:E\d{1,3})?\b', plain, re.I)
        resolution_hint = re.search(r'\b(?:2160p|1080p|720p|480p)\b', plain, re.I)
        if release_hint or episode_hint or resolution_hint:
            return plain
    return ''


def _rar_template_match_score(rar_name, template_info):
    rar_key = _pair_key_from_filename(rar_name)
    if not rar_key:
        return 0
    raw_rar = Path(str(rar_name or '')).stem.lower()
    rar_tokens = set(re.findall(r'[a-z0-9]{2,}', raw_rar))
    score = 0
    candidates = [
        template_info.get('source_name', ''),
        template_info.get('release_title', ''),
        template_info.get('first_title', ''),
    ]
    bundle_files = template_info.get('bundle_files') or []
    if bundle_files:
        candidates.append(bundle_files[0])
    for raw in [c for c in candidates if c]:
        cand = _pair_key_from_filename(raw)
        if cand == rar_key:
            return 100
        if cand and (cand in rar_key or rar_key in cand):
            score = max(score, 88)
            continue
        raw_tokens = set(re.findall(r'[a-z0-9]{2,}', str(raw).lower()))
        overlap = len(rar_tokens & raw_tokens)
        if overlap:
            ratio = overlap / max(1, min(len(rar_tokens), len(raw_tokens)))
            score = max(score, int(45 + ratio * 45))
    return score


def _match_label_for_score(score):
    try:
        score = int(score or 0)
    except Exception:
        score = 0
    if score >= 100:
        return 'filename'
    if score >= 85:
        return 'template_content_high'
    if score >= 65:
        return 'template_content_medium'
    if score >= 40:
        return 'template_content_low'
    return 'unmatched'


def _parse_human_size_to_bytes(value):
    raw = str(value or "").strip().replace(",", "")
    if not raw:
        return 0
    m = re.match(r"(?i)^([0-9]+(?:\.[0-9]+)?)\s*([kmgtp]?i?b)$", raw)
    if not m:
        return 0
    amount = float(m.group(1))
    unit = m.group(2).lower()
    powers = {
        "kb": 1000,
        "mb": 1000 ** 2,
        "gb": 1000 ** 3,
        "tb": 1000 ** 4,
        "kib": 1024,
        "mib": 1024 ** 2,
        "gib": 1024 ** 3,
        "tib": 1024 ** 4,
    }
    return int(amount * powers.get(unit, 1))


def _safe_line_capture(pattern, text):
    match = re.search(pattern, text, re.I | re.M)
    return match.group(1).strip() if match else ""


def _normalize_video_codec(text):
    src = str(text or "").lower()
    if not src:
        return ""
    if any(tok in src for tok in ["hevc", "h.265", "h265", "x265"]):
        return "HEVC / H.265"
    if any(tok in src for tok in ["avc", "h.264", "h264", "x264"]):
        return "AVC / H.264"
    return ""


def _normalize_audio_codec(text):
    src = str(text or "").lower()
    if not src:
        return ""
    if "atmos" in src and "e-ac-3" in src or "ddp" in src or "eac3" in src:
        return "DDP Atmos"
    if "e-ac-3" in src or "eac3" in src or "ddp" in src:
        return "DDP"
    if "truehd" in src and "atmos" in src:
        return "TrueHD Atmos"
    if "truehd" in src:
        return "TrueHD"
    if "dts-hd" in src:
        return "DTS-HD"
    if re.search(r'\baac\b', src):
        return "AAC"
    if "ac-3" in src or re.search(r'\bac3\b', src):
        return "AC3"
    return ""


def _detect_hdr_flags(*parts):
    joined = " \n".join(str(p or "") for p in parts).lower()
    flags = []
    if "dolby vision" in joined or re.search(r'\bdv\b', joined):
        flags.append("DV")
    if "hdr10+" in joined:
        flags.append("HDR10+")
    elif "hdr10" in joined or re.search(r'\bhdr\b', joined):
        flags.append("HDR10")
    return flags


def _episode_range(season_numbers, episode_numbers):
    if len(season_numbers) != 1 or not episode_numbers:
        return ""
    season = season_numbers[0]
    if len(episode_numbers) == 1:
        return f"S{season:02d}E{episode_numbers[0]:02d}"
    return f"S{season:02d}E{episode_numbers[0]:02d}-E{episode_numbers[-1]:02d}"


def _parse_template_info_text(text, source_name):
    header = _safe_line_capture(r"^Header:[^\S\r\n]*([^\r\n]*)$", text)
    groups = _safe_line_capture(r"^Group:[^\S\r\n]*([^\r\n]*)$", text)
    password = _safe_line_capture(r"^\[CODE\]([\s\S]*?)\[/CODE\]\s*$", text)
    declared_size = _safe_line_capture(r"^Size:[^\S\r\n]*([^\r\n]+)$", text)
    release_title = _template_release_title(text)
    summary_titles = re.findall(r"^Title:\s*(.+?)\s*$", text, re.I | re.M)
    first_title = summary_titles[0].strip() if summary_titles else ""
    episode_codes = re.findall(r"\bS(\d{1,2})E(\d{1,3})\b", text, re.I)
    season_numbers = sorted({int(s) for s, _e in episode_codes})
    episode_numbers = sorted({int(e) for _s, e in episode_codes})
    file_list = re.findall(r"^Files in job folder:\s*$([\s\S]*?)^\[/SPOILER\]", text, re.I | re.M)
    bundle_files = [line.strip() for line in file_list[0].splitlines() if line.strip()] if file_list else []
    audio_summary = _safe_line_capture(r"^Audio:\s*([^\r\n]+)$", text)
    video_summary = _safe_line_capture(r"^Video:\s*([^\r\n]+)$", text)
    duration_summary = _safe_line_capture(r"^Duration:\s*([^\r\n]+)$", text)
    imdb_id = _safe_line_capture(r"^IMDB\s*:\s*([^\r\n]+)$", text)
    tmdb_id = _safe_line_capture(r"^TMDB\s*:\s*([^\r\n]+)$", text)
    tvdb_id = _safe_line_capture(r"^TVDB\s*:\s*([^\r\n]+)$", text)
    groups_csv = groups
    if not groups_csv:
        m = re.search(r"^Group:[^\S\r\n]*$", text, re.I | re.M)
        if m:
            remainder = text[m.end():]
            next_line = remainder.splitlines()[1] if remainder.splitlines()[:2] else ""
            if next_line and not re.match(r"^(Size:|\[NZB\]|Password:|\[CODE\]|\[/HIDETHANKS\]|\[/SPOILER\])", next_line.strip(), re.I):
                groups_csv = next_line.strip()
    hdr_flags = _detect_hdr_flags(first_title, video_summary, text[:4000], source_name)
    video_codec = _normalize_video_codec(" \n".join([first_title, video_summary, source_name]))
    audio_codec = _normalize_audio_codec(" \n".join([first_title, audio_summary, source_name]))
    return {
        "text": text,
        "header": header,
        "groups": groups_csv,
        "password": password,
        "declared_size": declared_size,
        "declared_size_bytes": _parse_human_size_to_bytes(declared_size),
        "release_title": release_title,
        "source_name": source_name,
        "first_title": first_title,
        "episode_count": len({(int(s), int(e)) for s, e in episode_codes}),
        "season_numbers": season_numbers,
        "episode_numbers": episode_numbers,
        "episode_range": _episode_range(season_numbers, episode_numbers),
        "bundle_files": bundle_files,
        "audio_summary": audio_summary,
        "video_summary": video_summary,
        "duration_summary": duration_summary,
        "audio_codec": audio_codec,
        "video_codec": video_codec,
        "hdr_flags": hdr_flags,
        "imdb_id": imdb_id,
        "tmdb_id": tmdb_id,
        "tvdb_id": tvdb_id,
    }


def _template_info_cache_key(template_path: Path):
    path = Path(template_path)
    try:
        stat = path.stat()
        return str(path), int(stat.st_mtime_ns), int(stat.st_size)
    except OSError:
        return str(path), None, None


@lru_cache(maxsize=2048)
def _parse_template_info_cached(path_str, mtime_ns, size_bytes):
    template_path = Path(path_str)
    text = template_path.read_text(encoding="utf-8", errors="replace") if mtime_ns is not None else ""
    return _parse_template_info_text(text, template_path.name)


def parse_template_info(template_path: Path):
    info = _parse_template_info_cached(*_template_info_cache_key(template_path))
    result = dict(info)
    for key in ("season_numbers", "episode_numbers", "bundle_files", "hdr_flags"):
        result[key] = list(info.get(key) or [])
    return result


def infer_release_metadata(name, size_bytes=0, template_groups=""):
    src = str(name or "")
    match = SEASON_EP_RE.search(src)
    year = YEAR_RE.search(src)
    res = RES_RE.search(src)
    media_type = "tv" if match else ("movie" if year and MOVIE_HINTS.search(src) else ("movie" if year and not match else "unknown"))
    resolution = (res.group(1).lower() if res else "")
    if media_type == "tv":
        if resolution == "2160p":
            cat = "tv_uhd"
        elif resolution in {"1080p", "720p"}:
            cat = "tv_hd"
        else:
            cat = "tv_sd"
    else:
        if resolution == "2160p":
            cat = "movie_uhd"
        elif resolution in {"1080p", "720p"}:
            cat = "movie_hd"
        else:
            cat = "movie_sd"
    return {
        "media_type": media_type,
        "season": (match.group(1).zfill(2) if match else ""),
        "episode": (match.group(2).zfill(2) if match and match.group(2) else ""),
        "year": (year.group(1) if year else ""),
        "resolution": resolution,
        "category_key": cat,
        "groups_csv": template_groups,
        "size_bytes": int(size_bytes or 0),
    }


def _file_sha256(path):
    return _file_sha256_cached(*_file_hash_cache_key(path))


def _file_hash_cache_key(path):
    file_path = Path(path)
    stat = file_path.stat()
    return str(file_path), int(stat.st_mtime_ns), int(stat.st_size)


@lru_cache(maxsize=2048)
def _file_sha256_cached(path_str, mtime_ns, size_bytes):
    h = hashlib.sha256()
    with open(path_str, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _category_choice_for_destination(destination, category_key):
    cache = destination.get("categories_cache") or []
    std_id, std_label = STANDARD_CATEGORY_MAP.get(category_key, ("", category_key))
    overrides = destination.get("category_overrides") if isinstance(destination.get("category_overrides"), dict) else {}
    override_id = str(overrides.get(category_key, "") or "").strip()
    if override_id:
        for item in cache:
            if str(item.get("id", "")) == override_id:
                return str(item.get("id", "")), str(item.get("label") or std_label)
        return override_id, f"Override ({category_key})"
    for item in cache:
        if str(item.get("id", "")) == std_id:
            return str(item.get("id", "")), str(item.get("label") or std_label)
    for item in cache:
        label = str(item.get("label", "")).lower()
        if category_key.endswith("uhd") and ("uhd" in label or "2160" in label):
            return str(item.get("id", "")), str(item.get("label") or std_label)
        if category_key.endswith("hd") and ("hd" in label or "1080" in label or "720" in label):
            return str(item.get("id", "")), str(item.get("label") or std_label)
        if category_key.startswith("tv") and ("tv" in label or "series" in label):
            return str(item.get("id", "")), str(item.get("label") or std_label)
        if category_key.startswith("movie") and ("movie" in label or "film" in label):
            return str(item.get("id", "")), str(item.get("label") or std_label)
    return std_id, std_label


def resolve_category_preview_for_destination(destination, category_key):
    category_id, category_label = _category_choice_for_destination(destination, category_key)
    match_source = "standard"
    overrides = destination.get("category_overrides") if isinstance(destination.get("category_overrides"), dict) else {}
    if str(overrides.get(category_key, "") or "").strip():
        match_source = "override"
    elif destination.get("categories_cache"):
        match_source = "caps"
    return {
        "category_key": category_key,
        "selected_category_id": category_id,
        "selected_category_label": category_label,
        "match_source": match_source,
    }


def build_resolved_category_preview(destinations, category_key):
    results = []
    for destination in destinations:
        if not destination.get("enabled", True):
            continue
        resolved = resolve_category_preview_for_destination(destination, category_key)
        resolved.update({
            "destination_id": destination.get("id", ""),
            "destination_name": destination.get("name", destination.get("id", "")),
        })
        results.append(resolved)
    return results


def fetch_destination_caps(destination, timeout=30):
    base_url = normalize_service_base_url(destination.get("base_url", ""))
    api_key = str(destination.get("api_key", "") or "").strip()
    if not base_url or not api_key:
        raise RuntimeError("Destination base_url and api_key are required")
    url = f"{base_url}/api?" + urlencode({"t": "caps", "apikey": api_key})
    auth = None
    if destination.get("basic_auth") and (destination.get("username") or destination.get("password")):
        auth = (destination.get("username", ""), destination.get("password", ""))
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    categories = []
    for cat in root.findall(".//category"):
        cat_id = str(cat.attrib.get("id", "") or "")
        cat_name = str(cat.attrib.get("name", "") or "")
        if cat_id:
            categories.append({"id": cat_id, "label": cat_name})
        for sub in cat.findall("./subcat"):
            sid = str(sub.attrib.get("id", "") or "")
            sname = str(sub.attrib.get("name", "") or "")
            if sid:
                categories.append({"id": sid, "label": f"{cat_name} / {sname}" if cat_name else sname})
    seen = set()
    dedup = []
    for item in categories:
        key = (item["id"], item["label"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)
    return dedup


def refresh_share_caps(settings=None):
    settings = settings or _now_settings()
    destinations = get_share_destinations(settings)
    results = []
    for destination in destinations:
        result = {"id": destination.get("id"), "name": destination.get("name")}
        if not destination.get("enabled", True):
            result["status"] = "skipped"
            result["message"] = "Disabled"
            results.append(result)
            continue
        try:
            categories = fetch_destination_caps(destination, timeout=15)
            destination["categories_cache"] = categories
            result["status"] = "ok"
            result["count"] = len(categories)
        except Exception as exc:
            result["status"] = "error"
            result["message"] = str(exc)
        results.append(result)
    save_share_destinations(destinations, settings)
    return results


def build_share_candidates(settings=None):
    settings = settings or _now_settings()
    hidden_ids = get_hidden_share_candidate_ids(settings)
    history_rows = list_share_history(10000)
    shared_by_source = {}
    for job in history_rows:
        if str(job.get("status", "")).lower() != "done":
            continue
        source_ref = str(job.get("source_ref_id") or "")
        if not source_ref:
            continue
        shared_by_source.setdefault(source_ref, []).append({
            "destination_id": str(job.get("destination_id") or ""),
            "destination_name": str(job.get("destination_name") or job.get("destination_id") or ""),
        })
    results = []
    for row in list_posting_history(5000):
        if str(row.get("status", "")).lower() != "done":
            continue
        nzb_rar_path = str(row.get("nzb_rar_path") or row.get("nzb_path") or "")
        template_path = str(row.get("template_path") or "")
        if not nzb_rar_path or not template_path:
            continue
        info = parse_template_info(Path(template_path))
        effective_size = max(int(row.get("size_bytes") or 0), int(info.get("declared_size_bytes") or 0))
        meta = infer_release_metadata(row.get("job_name", ""), effective_size, info.get("groups", ""))
        candidate_id = f"posting:{row['id']}"
        if candidate_id in hidden_ids:
            continue
        results.append({
            "candidate_id": candidate_id,
            "source_type": "posting",
            "source_ref_id": f"posting:{row['id']}",
            "posting_job_id": row["id"],
            "job_name": row.get("job_name", ""),
            "release_name": row.get("job_name", ""),
            "nzb_rar_path": nzb_rar_path,
            "template_path": template_path,
            "size_bytes": effective_size,
            "groups_csv": info.get("groups", ""),
            "template_first_title": info.get("first_title", ""),
            "episode_count": int(info.get("episode_count") or 0),
            "template_declared_size": info.get("declared_size", ""),
            "audio_summary": info.get("audio_summary", ""),
            "video_summary": info.get("video_summary", ""),
            "audio_codec": info.get("audio_codec", ""),
            "video_codec": info.get("video_codec", ""),
            "hdr_flags": info.get("hdr_flags", []),
            "episode_range": info.get("episode_range", ""),
            "matched_by": row.get("matched_by", ""),
            "match_score": int(row.get("match_score") or 0),
            "shared_destinations": shared_by_source.get(f"posting:{row['id']}", []),
            **meta,
        })
    for row in list_imported_share_bundles(5000):
        info = parse_template_info(Path(row.get("template_path") or ""))
        effective_size = max(int(row.get("size_bytes") or 0), int(info.get("declared_size_bytes") or 0))
        meta = infer_release_metadata(row.get("release_name", ""), effective_size, info.get("groups", ""))
        candidate_id = f"import:{row['id']}"
        if candidate_id in hidden_ids:
            continue
        results.append({
            "candidate_id": candidate_id,
            "source_type": "import",
            "source_ref_id": f"import:{row['id']}",
            "import_bundle_id": row["id"],
            "job_name": row.get("release_name", ""),
            "release_name": row.get("release_name", ""),
            "nzb_rar_path": row.get("nzb_rar_path", ""),
            "template_path": row.get("template_path", ""),
            "mediainfo_override_path": row.get("mediainfo_override_path", ""),
            "size_bytes": effective_size,
            "groups_csv": info.get("groups", ""),
            "template_first_title": info.get("first_title", ""),
            "episode_count": int(info.get("episode_count") or 0),
            "template_declared_size": info.get("declared_size", ""),
            "audio_summary": info.get("audio_summary", ""),
            "video_summary": info.get("video_summary", ""),
            "audio_codec": info.get("audio_codec", ""),
            "video_codec": info.get("video_codec", ""),
            "hdr_flags": info.get("hdr_flags", []),
            "episode_range": info.get("episode_range", ""),
            "matched_by": row.get("matched_by", ""),
            "match_score": int(row.get("match_score") or 0),
            "shared_destinations": shared_by_source.get(f"import:{row['id']}", []),
            **meta,
        })
    return results


def import_share_bundle(nzb_rar_file, template_file, mediainfo_file=None, release_name="", matched_by="", match_score=0):
    settings = _now_settings()
    base = Path(settings.get("share_import_root") or "/media/dest/_share/imports")
    base.mkdir(parents=True, exist_ok=True)
    token = hashlib.sha1(os.urandom(24)).hexdigest()[:12]
    target = base / token
    target.mkdir(parents=True, exist_ok=True)
    nzb_rar_path = target / Path(nzb_rar_file.filename or "archive.rar").name
    template_path = target / Path(template_file.filename or "template.txt").name
    nzb_rar_file.save(str(nzb_rar_path))
    template_file.save(str(template_path))
    mediainfo_path = ""
    if mediainfo_file and getattr(mediainfo_file, "filename", ""):
        p = target / Path(mediainfo_file.filename).name
        mediainfo_file.save(str(p))
        mediainfo_path = str(p)
    rel = (release_name or "").strip() or nzb_rar_path.stem
    info = parse_template_info(template_path)
    effective_size = max(int(nzb_rar_path.stat().st_size), int(info.get("declared_size_bytes") or 0))
    bundle_id = create_imported_share_bundle(rel, str(nzb_rar_path), str(template_path), mediainfo_path, effective_size, matched_by=matched_by, match_score=match_score)
    return bundle_id


def import_share_bundles_bulk(nzb_rar_files, template_files, mediainfo_files=None):
    mediainfo_files = mediainfo_files or []
    templates = []
    for tf in template_files:
        try:
            raw = tf.read()
            if hasattr(tf, 'stream'):
                tf.stream.seek(0)
        except Exception:
            raw = b''
        try:
            text = raw.decode('utf-8', errors='replace')
        except Exception:
            text = ''
        info = parse_template_info(Path('/__virtual__/' + Path(tf.filename or 'template.txt').name)) if False else None
        info = {
            'source_name': Path(tf.filename or 'template.txt').name,
            'release_title': _template_release_title(text),
            'first_title': (re.findall(r'^Title:\s*(.+?)\s*$', text, re.I | re.M) or [''])[0].strip(),
            'bundle_files': re.findall(r'^Files in job folder:\s*$([\s\S]*?)^\[/SPOILER\]', text, re.I | re.M),
        }
        bundle_files = []
        if info['bundle_files']:
            bundle_files = [line.strip() for line in info['bundle_files'][0].splitlines() if line.strip()]
        info['bundle_files'] = bundle_files
        templates.append({'file': tf, 'raw': raw, 'info': info, 'paired': False})

    mediainfo_map = {}
    for mf in mediainfo_files:
        mediainfo_map.setdefault(_pair_key_from_filename(getattr(mf, 'filename', '')), []).append(mf)

    imported = []
    unmatched_rars = []
    pairings = []
    for rar in nzb_rar_files:
        rar_key = _pair_key_from_filename(getattr(rar, 'filename', ''))
        best = None
        best_score = -1
        for idx, entry in enumerate(templates):
            if entry['paired']:
                continue
            score = 0
            template_name_key = _pair_key_from_filename(entry['file'].filename or '')
            release_key = _pair_key_from_filename(entry['info'].get('release_title', ''))
            first_title_key = _pair_key_from_filename(entry['info'].get('first_title', ''))
            if rar_key and rar_key in {template_name_key, release_key, first_title_key}:
                score = 100
            else:
                score = _rar_template_match_score(rar.filename or '', entry['info'])
            if score > best_score:
                best = idx
                best_score = score
        if best is None or best_score < 40:
            unmatched_rars.append(getattr(rar, 'filename', ''))
            continue
        entry = templates[best]
        entry['paired'] = True
        mi = None
        for key in (rar_key, _pair_key_from_filename(entry['file'].filename or ''), _pair_key_from_filename(entry['info'].get('release_title', ''))):
            arr = mediainfo_map.get(key) or []
            if arr:
                mi = arr.pop(0)
                break
        release_name = (entry['info'].get('release_title') or '').strip() or Path(getattr(rar, 'filename', '')).stem
        match_label = _match_label_for_score(best_score)
        bundle_id = import_share_bundle(rar, entry['file'], mi, release_name, matched_by=match_label, match_score=best_score)
        imported.append(bundle_id)
        pairings.append({
            'nzb_rar': getattr(rar, 'filename', ''),
            'template': getattr(entry['file'], 'filename', ''),
            'match_score': best_score,
            'matched_by': match_label,
            'bundle_id': bundle_id,
            'confidence_score': best_score,
        })
    unmatched_templates = [getattr(entry['file'], 'filename', '') for entry in templates if not entry['paired']]
    return {
        'imported_count': len(imported),
        'bundle_ids': imported,
        'pairings': pairings,
        'unmatched_rars': unmatched_rars,
        'unmatched_templates': unmatched_templates,
    }


def generate_nfo_text(candidate, template_info):
    meta = infer_release_metadata(candidate.get("release_name") or candidate.get("job_name"), candidate.get("size_bytes") or 0, template_info.get("groups", ""))
    size_gib = float(meta.get("size_bytes") or 0) / (1024 ** 3)
    lines = [
        f"Release Name : {candidate.get('release_name') or candidate.get('job_name')}",
        "Created By   : PrepaC",
        f"Category     : {STANDARD_CATEGORY_MAP.get(candidate.get('category_key') or meta.get('category_key'), ('', candidate.get('category_key') or meta.get('category_key')))[1]}",
        f"Resolution   : {meta.get('resolution') or 'Unknown'}",
        f"Type         : {(meta.get('media_type') or 'Unknown').upper()}",
        f"Season       : {meta.get('season') or 'N/A'}",
        f"Episode      : {meta.get('episode') or 'N/A'}",
        f"Episodes     : {int(template_info.get('episode_count') or 0) or 'N/A'}",
        f"Episode Range: {template_info.get('episode_range') or 'N/A'}",
        f"Year         : {meta.get('year') or 'N/A'}",
        f"Size         : {size_gib:.2f} GiB",
        f"Password     : {'Included in NZB' if template_info.get('password') else 'None detected'}",
        "Posted With  : PrepaC",
    ]
    if template_info.get('hdr_flags'):
        lines.append(f"HDR Flags    : {', '.join(template_info.get('hdr_flags') or [])}")
    if template_info.get('video_codec'):
        lines.append(f"Video Codec  : {template_info.get('video_codec')}")
    if template_info.get('audio_codec'):
        lines.append(f"Audio Codec  : {template_info.get('audio_codec')}")
    if template_info.get('video_summary'):
        lines.append(f"Video        : {template_info.get('video_summary')}")
    if template_info.get('audio_summary'):
        lines.append(f"Audio        : {template_info.get('audio_summary')}")
    if template_info.get('duration_summary'):
        lines.append(f"Duration     : {template_info.get('duration_summary')}")
    for label, key in (("IMDB", "imdb_id"), ("TMDB", "tmdb_id"), ("TVDB", "tvdb_id")):
        if template_info.get(key):
            lines.append(f"{label:<12}: {template_info.get(key)}")
    lines.extend(["", "Groups Used", "-----------"])
    groups = [g.strip() for g in str(template_info.get("groups", "")).split(",") if g.strip()]
    if groups:
        lines.extend(groups)
    else:
        lines.append("Unknown")
    if template_info.get('first_title'):
        lines.extend(["", "First File", "----------", template_info.get('first_title')])
    lines.extend([
        "",
        "Description",
        "-----------",
        "This release was prepared, packed, posted, and shared using PrepaC.",
        "",
        "Notes",
        "-----",
        "- Category was auto-detected and may have been adjusted before submission.",
        "- MediaInfo XML is attached when available.",
        "- Generated NFO created by PrepaC.",
    ])
    return "\n".join(lines) + "\n"


def generate_metadata_xml(candidate, template_info):
    meta = infer_release_metadata(candidate.get("release_name") or candidate.get("job_name"), candidate.get("size_bytes") or 0, template_info.get("groups", ""))
    groups = [g.strip() for g in str(template_info.get("groups", "")).split(",") if g.strip()]
    root = ET.Element("MediaInfo")
    general = ET.SubElement(root, "General")
    general_fields = {
        "ReleaseName": candidate.get("release_name") or candidate.get("job_name"),
        "MediaType": meta.get("media_type") or "",
        "Resolution": meta.get("resolution") or "",
        "Year": meta.get("year") or "",
        "Season": meta.get("season") or "",
        "Episode": meta.get("episode") or "",
        "EpisodeCount": int(template_info.get("episode_count") or 0),
        "EpisodeRange": template_info.get("episode_range", ""),
        "SizeBytes": int(meta.get("size_bytes") or 0),
        "Header": template_info.get("header", ""),
        "PasswordIncluded": "true" if template_info.get("password") else "false",
        "FirstTitle": template_info.get("first_title", ""),
        "HDRFlags": ",".join(template_info.get("hdr_flags") or []),
        "VideoCodec": template_info.get("video_codec", ""),
        "AudioCodec": template_info.get("audio_codec", ""),
        "VideoSummary": template_info.get("video_summary", ""),
        "AudioSummary": template_info.get("audio_summary", ""),
        "DurationSummary": template_info.get("duration_summary", ""),
        "IMDB": template_info.get("imdb_id", ""),
        "TMDB": template_info.get("tmdb_id", ""),
        "TVDB": template_info.get("tvdb_id", ""),
    }
    for tag, value in general_fields.items():
        child = ET.SubElement(general, tag)
        child.text = str(value)

    usenet = ET.SubElement(root, "Usenet")
    groups_node = ET.SubElement(usenet, "Groups")
    for group in groups:
        child = ET.SubElement(groups_node, "Group")
        child.text = group

    return ET.tostring(root, encoding="utf-8", xml_declaration=True, short_empty_elements=False).decode("utf-8") + "\n"


def _extract_nzb_from_rar(rar_path: Path, workdir: Path):
    workdir.mkdir(parents=True, exist_ok=True)
    commands = [
        ["rar", "x", "-idq", str(rar_path), str(workdir)],
        ["unrar", "x", "-idq", str(rar_path), str(workdir)],
    ]
    success = False
    for cmd in commands:
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            success = True
            break
        except Exception:
            continue
    if not success:
        raise RuntimeError("Could not extract NZB from RAR archive")
    nzbs = list(workdir.rglob("*.nzb"))
    if not nzbs:
        raise RuntimeError("Could not locate extracted NZB file")
    return nzbs[0]


def _build_candidate_for_job(job):
    return {
        "candidate_id": job.get("source_ref_id", ""),
        "source_type": job.get("source_type", ""),
        "source_ref_id": job.get("source_ref_id", ""),
        "posting_job_id": job.get("posting_job_id"),
        "import_bundle_id": job.get("import_bundle_id"),
        "job_name": job.get("job_name", ""),
        "release_name": job.get("release_name", ""),
        "nzb_rar_path": job.get("nzb_rar_path", ""),
        "template_path": job.get("template_path", ""),
        "size_bytes": 0,
        "category_key": job.get("category_key", ""),
    }


def _persist_share_artifacts(job, nfo_path, xml_path):
    try:
        rar_path = Path(job.get("nzb_rar_path") or "")
        base_dir = rar_path.parent if rar_path.parent.exists() else Path(job.get("template_path") or "").parent
        if not str(base_dir):
            return "", ""
        target_dir = base_dir / "_share"
        target_dir.mkdir(parents=True, exist_ok=True)
        stem = rar_path.stem or Path(job.get("job_name") or job.get("release_name") or "share_artifact").stem or "share_artifact"
        final_nfo = target_dir / f"{stem}.nfo"
        final_xml = target_dir / f"{stem}.xml"
        shutil.copy2(nfo_path, final_nfo)
        shutil.copy2(xml_path, final_xml)
        return str(final_nfo), str(final_xml)
    except Exception:
        return "", ""


def run_share_job(job_id, settings=None):
    settings = settings or _now_settings()
    job = get_share_job(job_id)
    if not job:
        return
    destinations = {d.get("id"): d for d in get_share_destinations(settings)}
    destination = destinations.get(job.get("destination_id"))
    if not destination:
        finish_share(job_id, False, "Destination not found")
        return
    ACTIVE_SHARE_JOB_IDS.add(int(job_id))
    try:
        update_share_job(job_id, status="running", started_at=__import__("datetime").datetime.now().isoformat(timespec="seconds"))
        if get_share_job_status(job_id).lower() == "cancelled":
            return
        add_share_event(job_id, "prepare", "Preparing share payload", 5)
        template_info = parse_template_info(Path(job.get("template_path") or ""))
        with tempfile.TemporaryDirectory(prefix="prepac_share_") as td:
            workdir = Path(td)
            extracted_nzb = _extract_nzb_from_rar(Path(job["nzb_rar_path"]), workdir / "extract")
            if get_share_job_status(job_id).lower() == "cancelled":
                return
            add_share_event(job_id, "prepare", "Extracted NZB from RAR", 15)
            nfo_path = workdir / f"{Path(job['job_name']).stem}.nfo"
            nfo_path.write_text(generate_nfo_text(_build_candidate_for_job(job), template_info), encoding="utf-8")
            xml_path = workdir / f"{Path(job['job_name']).stem}.xml"
            xml_path.write_text(generate_metadata_xml(_build_candidate_for_job(job), template_info), encoding="utf-8")
            update_share_job(job_id, generated_nfo_path=str(nfo_path), generated_mediainfo_path=str(xml_path))
            if get_share_job_status(job_id).lower() == "cancelled":
                return
            add_share_event(job_id, "upload", "Submitting NZB to destination", 35)
            api_key = str(destination.get("api_key", "") or "").strip()
            base_url = normalize_service_base_url(destination.get("base_url", ""))
            cat_id = str(job.get("selected_category_id") or "")
            query = {
                "t": "nzbadd",
                "apikey": api_key,
                "cat": cat_id,
                "includemeta": "true" if destination.get("includemeta", True) else "false",
            }
            url = f"{base_url}/api?" + urlencode(query)
            files = {}
            try:
                files = {"file": (extracted_nzb.name, open(extracted_nzb, "rb"), "application/x-nzb")}
                if destination.get("include_nfo", True):
                    files["nfo"] = (nfo_path.name, open(nfo_path, "rb"), "text/plain")
                if destination.get("include_mediainfo", True):
                    files["mediainfo"] = (xml_path.name, open(xml_path, "rb"), "application/xml")
                auth = None
                if destination.get("basic_auth") and (destination.get("username") or destination.get("password")):
                    auth = (destination.get("username", ""), destination.get("password", ""))
                timeout = int(str(settings.get("share_request_timeout", "120") or "120"))
                r = requests.post(url, files=files, timeout=timeout, auth=auth)
                body = r.text
            finally:
                for _, file_tuple in files.items():
                    try:
                        file_tuple[1].close()
                    except Exception:
                        pass
            if get_share_job_status(job_id).lower() == "cancelled":
                return
            if not r.ok:
                raise RuntimeError(f"Share upload failed: HTTP {r.status_code} - {body[:500]}")
            remote_id = ""
            remote_guid = ""
            try:
                xroot = ET.fromstring(body)
                remote_id = xroot.attrib.get("id", "") or xroot.findtext(".//item/id") or ""
                remote_guid = xroot.attrib.get("guid", "") or xroot.findtext(".//item/guid") or ""
            except Exception:
                pass
            persisted_nfo_path, persisted_xml_path = _persist_share_artifacts(job, nfo_path, xml_path)
            update_fields = {"raw_response": body[:4000], "remote_id": remote_id, "remote_guid": remote_guid}
            if persisted_nfo_path:
                update_fields["generated_nfo_path"] = persisted_nfo_path
            if persisted_xml_path:
                update_fields["generated_mediainfo_path"] = persisted_xml_path
            update_share_job(job_id, **update_fields)
            if get_share_job_status(job_id).lower() == "cancelled":
                return
            add_share_event(job_id, "complete", "Destination accepted share upload", 100)
            finish_share(job_id, True, "Share complete")
    except Exception as exc:
        finish_share(job_id, False, str(exc))
        add_share_event(job_id, "failed", str(exc), None)
    finally:
        ACTIVE_SHARE_JOB_IDS.discard(int(job_id))


def start_share_job_async(job_id, settings=None):
    settings = settings or _now_settings()
    threading.Thread(target=run_share_job, args=(job_id, settings), daemon=True).start()
    return job_id


def queue_share_jobs(request_items, destination_ids, settings=None):
    settings = settings or _now_settings()
    destinations = {d.get("id"): d for d in get_share_destinations(settings) if d.get("enabled", True)}
    candidates = {c["candidate_id"]: c for c in build_share_candidates(settings)}
    queued = []
    skipped = []
    for item in request_items:
        candidate = candidates.get(item.get("candidate_id"))
        if not candidate:
            skipped.append({"candidate_id": item.get("candidate_id"), "reason": "Candidate not found"})
            continue
        category_key = item.get("category_key") or candidate.get("category_key") or "movie_hd"
        for dest_id in destination_ids:
            destination = destinations.get(dest_id)
            if not destination:
                skipped.append({"candidate_id": item.get("candidate_id"), "destination_id": dest_id, "reason": "Destination not found or disabled"})
                continue
            if get_existing_active_share_job_ids(candidate.get("source_ref_id", ""), dest_id):
                skipped.append({"candidate_id": item.get("candidate_id"), "destination_id": dest_id, "reason": "Active share already exists"})
                continue
            selected_cat_id, selected_cat_label = _category_choice_for_destination(destination, category_key)
            nzb_hash = _file_sha256(candidate["nzb_rar_path"]) if Path(candidate["nzb_rar_path"]).exists() else ""
            duplicate_checks = count_existing_share_duplicates(dest_id, candidate.get("job_name", ""), nzb_hash, candidate.get("source_ref_id", ""))
            if any(duplicate_checks.values()):
                skipped.append({
                    "candidate_id": item.get("candidate_id"),
                    "destination_id": dest_id,
                    "reason": f"Duplicate prevented ({', '.join(k for k, v in duplicate_checks.items() if v)})",
                })
                continue
            job_hash = hashlib.sha256(f"{dest_id}|{candidate.get('job_name', '')}|{nzb_hash}".encode()).hexdigest()
            job_id = create_share_job(
                source_type=candidate.get("source_type", ""),
                source_ref_id=candidate.get("source_ref_id", ""),
                posting_job_id=candidate.get("posting_job_id"),
                import_bundle_id=candidate.get("import_bundle_id"),
                job_name=candidate.get("job_name", ""),
                release_name=candidate.get("release_name", ""),
                nzb_rar_path=candidate.get("nzb_rar_path", ""),
                template_path=candidate.get("template_path", ""),
                detected_type=candidate.get("media_type", ""),
                resolution_tier=candidate.get("resolution", ""),
                category_key=category_key,
                selected_category_id=selected_cat_id,
                selected_category_label=selected_cat_label,
                destination_id=dest_id,
                destination_name=destination.get("name", dest_id),
                nzb_hash=nzb_hash,
                job_hash=job_hash,
            )
            start_share_job_async(job_id, settings)
            queued.append({"job_id": job_id, "candidate_id": item.get("candidate_id"), "destination_id": dest_id})
    return {"queued": queued, "skipped": skipped}


def build_share_submission_review(request_items, destination_ids, settings=None):
    settings = settings or _now_settings()
    destinations = {d.get("id"): d for d in get_share_destinations(settings) if d.get("enabled", True)}
    candidates = {c["candidate_id"]: c for c in build_share_candidates(settings)}
    reviews = []
    summary = {"ready": 0, "blocked": 0, "warnings": 0}
    for item in request_items:
        candidate = candidates.get(item.get("candidate_id"))
        if not candidate:
            summary["blocked"] += len(destination_ids or [None])
            for dest_id in destination_ids or [""]:
                reviews.append({
                    "candidate_id": item.get("candidate_id"),
                    "destination_id": dest_id or "",
                    "release_name": item.get("candidate_id") or "",
                    "status": "blocked",
                    "warning": "Candidate not found",
                    "nfo": False,
                    "mediainfo": False,
                })
            continue
        category_key = item.get("category_key") or candidate.get("category_key") or "movie_hd"
        nzb_hash = _file_sha256(candidate["nzb_rar_path"]) if Path(candidate.get("nzb_rar_path", "")).exists() else ""
        for dest_id in destination_ids:
            destination = destinations.get(dest_id)
            if not destination:
                summary["blocked"] += 1
                reviews.append({
                    "candidate_id": candidate.get("candidate_id", ""),
                    "destination_id": dest_id,
                    "release_name": candidate.get("release_name") or candidate.get("job_name") or "",
                    "status": "blocked",
                    "warning": "Destination not found or disabled",
                    "nfo": False,
                    "mediainfo": False,
                })
                continue
            selected_cat_id, selected_cat_label = _category_choice_for_destination(destination, category_key)
            duplicate_checks = count_existing_share_duplicates(dest_id, candidate.get("job_name", ""), nzb_hash, candidate.get("source_ref_id", ""))
            duplicate_reasons = [k for k, v in duplicate_checks.items() if v]
            active_exists = bool(get_existing_active_share_job_ids(candidate.get("source_ref_id", ""), dest_id))
            match = resolve_category_preview_for_destination(destination, category_key)
            warning_parts = []
            status = "ready"
            if active_exists:
                status = "blocked"
                warning_parts.append("Active share already exists")
            if duplicate_reasons:
                status = "blocked"
                warning_parts.append(f"Duplicate prevented ({', '.join(duplicate_reasons)})")
            if match.get("match_source") == "standard" and destination.get("categories_cache"):
                warning_parts.append("Using standard fallback category")
            if status == "ready" and warning_parts:
                summary["warnings"] += 1
            elif status == "ready":
                summary["ready"] += 1
            else:
                summary["blocked"] += 1
            reviews.append({
                "candidate_id": candidate.get("candidate_id", ""),
                "destination_id": dest_id,
                "destination_name": destination.get("name", dest_id),
                "release_name": candidate.get("release_name") or candidate.get("job_name") or "",
                "category_key": category_key,
                "selected_category_id": selected_cat_id,
                "selected_category_label": selected_cat_label,
                "match_source": match.get("match_source", "standard"),
                "nfo": bool(destination.get("include_nfo", True)),
                "mediainfo": bool(destination.get("include_mediainfo", True)),
                "status": status,
                "warning": "; ".join(warning_parts),
            })
    return {"reviews": reviews, "summary": summary}


def maybe_auto_share_posting_job(posting_job, settings=None):
    settings = settings or _now_settings()
    if not share_auto_after_posting_enabled(settings):
        return []
    destinations = [d for d in get_share_destinations(settings) if d.get("enabled", True) and str(d.get("mode", "manual")).lower() in {"auto", "both"}]
    if not destinations:
        return []
    meta = infer_release_metadata(posting_job.get("job_name", ""), posting_job.get("size_bytes") or 0, posting_job.get("groups_csv", ""))
    request_items = [{"candidate_id": f"posting:{posting_job.get('id')}", "category_key": meta.get("category_key")}]
    result = queue_share_jobs(request_items, [d.get("id") for d in destinations], settings)
    return result.get("queued", [])
