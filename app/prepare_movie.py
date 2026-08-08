from pathlib import Path
from app.helpers import sanitize_show_name, detect_bracket_info_from_filenames, scan_videos_nonrecursive, largest_video_file, is_trailer_file, enforce_name_length
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

def search_movies(movie_root, query):
    root = Path(movie_root); q = query.strip().lower()
    if not q or not root.exists(): return []
    assert_path_within_roots(root, [root], "movie root", allow_root=True, require_exists=True, require_directory=True)
    return sorted([p.name for p in root.iterdir() if p.is_dir() and not p.is_symlink() and q in p.name.lower()])

def preview_movie(settings, movie_name, bracket_override="", bracket_is_resolved=False):
    movie_name = _safe_child_name(movie_name, "movie name")
    movie_path = Path(settings["movie_root"]) / movie_name
    try:
        movie_path = assert_operation_path(
            movie_path, settings, "prepare_movie_source", "movie source",
            require_exists=True, require_directory=True,
        )
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    files = [p for p in scan_videos_nonrecursive(movie_path) if not is_trailer_file(p.name)]
    if not files: raise ValueError("No non-trailer video files found.")
    try:
        files = [
            assert_path_within_roots(
                video, [movie_path], "movie video", require_exists=True, require_directory=False,
            )
            for video in files
        ]
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    biggest = largest_video_file(files)
    if biggest is None: raise ValueError("Could not determine biggest non-trailer video file.")
    tags = detect_tags(str(biggest))
    bracket = bracket_override.strip()
    if not bracket and not bracket_is_resolved:
        bracket = detect_bracket_info_from_filenames([biggest.name])
        bracket = merge_bracket_with_detected_hdr(bracket, tags, "movie")
    queue_bracket = bracket
    dest_show = sanitize_show_name(movie_name)
    folder, _, chosen_bracket = enforce_name_length(dest_show, bracket, settings["end_tag"], int(settings["max_name_len"]), "")
    dest_path = prepare_root(settings) / folder
    try:
        movie_path, dest_path = assert_operation_pair(
            movie_path, dest_path, settings,
            "prepare_movie_source", "prepare_destination",
            source_label="movie source", destination_label="prepare destination",
        )
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    dest_path = str(dest_path)
    return {"media_type":"movie","movie_name":movie_name,"source_path":str(movie_path),"source_rel":movie_name,"source_file":str(biggest),
            "all_non_trailer_files":[str(p) for p in files],"detected_tags":tags,"queue_bracket":queue_bracket,
            "bracket_is_resolved":True,"chosen_bracket":chosen_bracket,"dest_folder":folder,
            "dest_path":dest_path,"path_warn":len(dest_path) > int(settings["win_path_warn"])}
