import os, re
from pathlib import Path

VIDEO_EXTS = ("mkv","mp4","avi","mov","m4v","ts","m2ts","wmv","mpg","mpeg","webm")
WINDOWS_RESERVED = {"CON","PRN","AUX","NUL","CLOCK$","CONIN$","CONOUT$",
                    *[f"COM{i}" for i in range(1,10)], *[f"LPT{i}" for i in range(1,10)]}

def trim(s): return s.strip()
def sanitize_show_name(name): return trim(re.sub(r"\s*\{[^}]*\}", "", name))
def season_key(s): return trim(s).lower()
def is_video_file(path: Path): return path.is_file() and path.suffix.lower().lstrip(".") in VIDEO_EXTS
def scan_videos_nonrecursive(path: Path): return sorted([p for p in path.iterdir() if is_video_file(p)], key=lambda p: p.name.lower())
def scan_videos_recursive(path: Path): return sorted([p for p in path.rglob("*") if is_video_file(p)], key=lambda p: str(p).lower())

def video_stats(paths):
    count, total = 0, 0
    for p in paths:
        if p.exists() and p.is_file():
            count += 1; total += p.stat().st_size
    return count, total

def human_bytes(num):
    units = ["B","KB","MB","GB","TB"]
    value = float(num)
    for u in units:
        if value < 1024 or u == units[-1]:
            return f"{value:.2f} {u}"
        value /= 1024
    return f"{num} B"

def strip_show_year(s): return re.sub(r"\s*\(\s*(19|20)\d{2}\s*\)\s*$|\s+(19|20)\d{2}\s*$", "", s).strip()
def remove_spaces(s): return re.sub(r"\s+", "", s)

def strip_last_codec_bracket(s):
    m = re.search(r"(.*)(\[[^\[\]]+\])$", s)
    if not m: return s
    inner = m.group(2).strip("[]").lower()
    if re.search(r"(^|[^a-z0-9])(x264|h264|x265|h265|hevc|h\.264|h\.265)([^a-z0-9]|$)", inner):
        return m.group(1).rstrip()
    return s

def windows_safe_name(s):
    s = re.sub(r'[<>:"/\\|?*]', "-", s)
    s = re.sub(r"\s+$", "", s)
    s = re.sub(r"\.+$", "", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip()
    if s.upper() in WINDOWS_RESERVED: s += "_"
    return s

def detect_bracket_info_from_filenames(names):
    for name in names:
        m = re.match(r"^[^\[]*((\[[^\]]+\])+).*$", name)
        if m: return m.group(1).strip()
    return ""

def is_trailer_file(name):
    s = os.path.basename(name).lower()
    return "-trailer." in s or "-trailer " in s or s.endswith("-trailer")

def largest_video_file(paths):
    best, best_size = None, -1
    for p in paths:
        size = p.stat().st_size
        if size > best_size:
            best = p; best_size = size
    return best

def build_dest_folder(show, bracket, end_tag, season_tag=""):
    if season_tag and bracket: out = f"{show} - {season_tag} - {bracket} - {end_tag}"
    elif season_tag and not bracket: out = f"{show} - {season_tag} - {end_tag}"
    elif bracket: out = f"{show} - {bracket} - {end_tag}"
    else: out = f"{show} - {end_tag}"
    return windows_safe_name(out)

def enforce_name_length(dest_show, dest_bracket, end_tag, max_name_len, season_tag=""):
    folder = build_dest_folder(dest_show, dest_bracket, end_tag, season_tag)
    if len(folder) <= max_name_len: return folder, dest_show, dest_bracket
    dest_show = remove_spaces(dest_show)
    folder = build_dest_folder(dest_show, dest_bracket, end_tag, season_tag)
    if len(folder) <= max_name_len: return folder, dest_show, dest_bracket
    dest_show = strip_show_year(dest_show)
    folder = build_dest_folder(dest_show, dest_bracket, end_tag, season_tag)
    if len(folder) <= max_name_len: return folder, dest_show, dest_bracket
    if dest_bracket: dest_bracket = trim(strip_last_codec_bracket(dest_bracket))
    folder = build_dest_folder(dest_show, dest_bracket, end_tag, season_tag)
    if len(folder) <= max_name_len: return folder, dest_show, dest_bracket
    fixed = f" - {season_tag} - {dest_bracket} - {end_tag}" if season_tag and dest_bracket else \
            f" - {season_tag} - {end_tag}" if season_tag else \
            f" - {dest_bracket} - {end_tag}" if dest_bracket else f" - {end_tag}"
    max_show_len = max_name_len - len(fixed)
    if max_show_len < 1: raise ValueError("Cannot fit destination folder name within maximum length.")
    dest_show = dest_show[:max_show_len]
    return windows_safe_name(dest_show + fixed), dest_show, dest_bracket

def normalize_name(s):
    s = sanitize_show_name(s)
    s = strip_show_year(s)
    s = re.sub(r"[\[\]\{\}\(\)\._-]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()
