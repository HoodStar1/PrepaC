from pathlib import Path
from app.helpers import sanitize_show_name, detect_bracket_info_from_filenames, scan_videos_nonrecursive, largest_video_file, is_trailer_file, enforce_name_length
from app.media_probe import detect_tags, build_bracket_from_detected

def search_movies(movie_root, query):
    root = Path(movie_root); q = query.strip().lower()
    if not q or not root.exists(): return []
    return sorted([p.name for p in root.iterdir() if p.is_dir() and q in p.name.lower()])

def preview_movie(settings, movie_name, bracket_override=""):
    movie_path = Path(settings["movie_root"]) / movie_name
    if not movie_path.exists(): raise ValueError("Movie path not found.")
    files = [p for p in scan_videos_nonrecursive(movie_path) if not is_trailer_file(p.name)]
    if not files: raise ValueError("No non-trailer video files found.")
    biggest = largest_video_file(files)
    if biggest is None: raise ValueError("Could not determine biggest non-trailer video file.")
    bracket = bracket_override.strip() or detect_bracket_info_from_filenames([biggest.name])
    tags = detect_tags(str(biggest))
    if not bracket: bracket = build_bracket_from_detected(tags, "movie")
    dest_show = sanitize_show_name(movie_name)
    folder, _, chosen_bracket = enforce_name_length(dest_show, bracket, settings["end_tag"], int(settings["max_name_len"]), "")
    dest_path = str(Path(settings["dest_root"]) / folder)
    return {"media_type":"movie","movie_name":movie_name,"source_path":str(movie_path),"source_rel":movie_name,"source_file":str(biggest),
            "all_non_trailer_files":[str(p) for p in files],"detected_tags":tags,"chosen_bracket":chosen_bracket,"dest_folder":folder,
            "dest_path":dest_path,"path_warn":len(dest_path) > int(settings["win_path_warn"])}
