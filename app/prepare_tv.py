from pathlib import Path
from app.helpers import sanitize_show_name, detect_bracket_info_from_filenames, season_key, scan_videos_nonrecursive, scan_videos_recursive, enforce_name_length
from app.media_probe import detect_tags, merge_bracket_with_detected_hdr
from app.workflow_paths import prepare_root
from app.path_guardrails import (
    assert_operation_pair,
    assert_operation_path,
    assert_path_within_roots,
)


def _safe_child_name(value, label):
    value = str(value or "").strip()
    if not value or value in {".", ".."} or Path(value).name != value or "/" in value or "\\" in value:
        raise ValueError(f"Invalid {label}.")
    return value

def search_shows(tv_root, query):
    root = Path(tv_root); q = query.strip().lower()
    if not q or not root.exists(): return []
    assert_path_within_roots(root, [root], "TV root", allow_root=True, require_exists=True, require_directory=True)
    seen = set()
    results = []
    for p in root.iterdir():
        if p.is_dir() and not p.is_symlink() and q in p.name.lower() and p.name not in seen:
            seen.add(p.name)
            results.append(p.name)
    return sorted(results)

def list_seasons(tv_root, show_name):
    show_name = _safe_child_name(show_name, "show name")
    show = Path(tv_root) / show_name
    if not show.exists(): return []
    assert_path_within_roots(show, [tv_root], "TV show", require_exists=True, require_directory=True)
    out = []
    for p in show.iterdir():
        if not p.is_dir() or p.is_symlink(): continue
        name = p.name; lk = season_key(name)
        if (name.startswith("Season ") and len(name) >= 9) or lk == "specials": out.append(name)
    return sorted(out)

def preview_tv(settings, show_name, season_name, bracket_override="", bracket_is_resolved=False):
    show_name = _safe_child_name(show_name, "show name")
    season_name = _safe_child_name(season_name, "season name")
    show_path = Path(settings["tv_root"]) / show_name
    season_path = show_path / season_name
    try:
        season_path = assert_operation_path(
            season_path, settings, "prepare_tv_source", "TV season",
            require_exists=True, require_directory=True,
        )
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    lk = season_key(season_name)
    files = scan_videos_nonrecursive(season_path)
    if not files and lk in ("specials", "season 00"): files = scan_videos_recursive(season_path)
    if not files: raise ValueError("No video files found.")
    try:
        files = [
            assert_path_within_roots(
                video, [season_path], "TV video", require_exists=True, require_directory=False,
            )
            for video in files
        ]
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    season_num = "00" if lk == "specials" else season_name.split()[-1]
    season_tag = f"S{season_num}"
    tags = detect_tags(str(files[0]))
    bracket = bracket_override.strip()
    if not bracket and not bracket_is_resolved:
        bracket = detect_bracket_info_from_filenames([p.name for p in files])
        bracket = merge_bracket_with_detected_hdr(bracket, tags, "tv")
    queue_bracket = bracket
    dest_show = sanitize_show_name(show_name)
    folder, _, chosen_bracket = enforce_name_length(dest_show, bracket, settings["end_tag"], int(settings["max_name_len"]), season_tag)
    dest_path = prepare_root(settings) / folder
    try:
        season_path, dest_path = assert_operation_pair(
            season_path, dest_path, settings,
            "prepare_tv_source", "prepare_destination",
            source_label="TV season", destination_label="prepare destination",
        )
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    dest_path = str(dest_path)
    return {"media_type":"tv","show_name":show_name,"season_name":season_name,"source_path":str(season_path),"source_rel":f"{show_name}/{season_name}",
            "season_tag":season_tag,"video_files":[str(p) for p in files],"detected_tags":tags,"queue_bracket":queue_bracket,
            "bracket_is_resolved":True,"chosen_bracket":chosen_bracket,
            "dest_folder":folder,"dest_path":dest_path,"path_warn":len(dest_path) > int(settings["win_path_warn"])}
