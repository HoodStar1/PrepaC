from pathlib import Path
from app.helpers import sanitize_show_name, detect_bracket_info_from_filenames, season_key, scan_videos_nonrecursive, scan_videos_recursive, enforce_name_length
from app.media_probe import detect_tags, build_bracket_from_detected

def search_shows(tv_root, query):
    root = Path(tv_root); q = query.strip().lower()
    if not q or not root.exists(): return []
    return sorted([p.name for p in root.iterdir() if p.is_dir() and q in p.name.lower()])

def list_seasons(tv_root, show_name):
    show = Path(tv_root) / show_name
    if not show.exists(): return []
    out = []
    for p in show.iterdir():
        if not p.is_dir(): continue
        name = p.name; lk = season_key(name)
        if (name.startswith("Season ") and len(name) >= 9) or lk == "specials": out.append(name)
    return sorted(out)

def preview_tv(settings, show_name, season_name, bracket_override=""):
    show_path = Path(settings["tv_root"]) / show_name
    season_path = show_path / season_name
    if not season_path.exists(): raise ValueError("Season path not found.")
    lk = season_key(season_name)
    files = scan_videos_nonrecursive(season_path)
    if not files and lk in ("specials", "season 00"): files = scan_videos_recursive(season_path)
    if not files: raise ValueError("No video files found.")
    season_num = "00" if lk == "specials" else season_name.split()[-1]
    season_tag = f"S{season_num}"
    bracket = bracket_override.strip() or detect_bracket_info_from_filenames([p.name for p in files])
    tags = detect_tags(str(files[0]))
    if not bracket: bracket = build_bracket_from_detected(tags, "tv")
    dest_show = sanitize_show_name(show_name)
    folder, _, chosen_bracket = enforce_name_length(dest_show, bracket, settings["end_tag"], int(settings["max_name_len"]), season_tag)
    dest_path = str(Path(settings["dest_root"]) / folder)
    return {"media_type":"tv","show_name":show_name,"season_name":season_name,"source_path":str(season_path),"source_rel":f"{show_name}/{season_name}",
            "season_tag":season_tag,"video_files":[str(p) for p in files],"detected_tags":tags,"chosen_bracket":chosen_bracket,
            "dest_folder":folder,"dest_path":dest_path,"path_warn":len(dest_path) > int(settings["win_path_warn"])}
