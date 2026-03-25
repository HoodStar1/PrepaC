import os, shutil, subprocess
from pathlib import Path
from app.helpers import human_bytes, video_stats
from app.jobs import add_job_event, set_job_status, finish_job
from app.history_db import save_prepared_item

VIDEO_EXTS = ("mkv","mp4","avi","mov","m4v","ts","m2ts","wmv","mpg","mpeg","webm")

def _run_rsync(cmd, job_id):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    if proc.stdout:
        for line in proc.stdout:
            msg = line.strip()
            if msg: add_job_event(job_id, "copying", msg[:400], None)
    return proc.wait()

def _chmod_chown(dest_path, settings):
    try:
        if os.path.isdir(dest_path):
            os.chmod(dest_path, 0o777)
        else:
            os.chmod(dest_path, 0o666)
    except Exception:
        pass
    try:
        shutil.chown(dest_path, user=settings.get("owner_user", ""), group=settings.get("owner_group", ""))
    except Exception:
        pass

def _apply_open_permissions_recursive(root_path):
    root_path = str(root_path)
    for current_root, dirnames, filenames in os.walk(root_path):
        for d in dirnames:
            try:
                os.chmod(os.path.join(current_root, d), 0o777)
            except Exception:
                pass
        for f in filenames:
            try:
                os.chmod(os.path.join(current_root, f), 0o666)
            except Exception:
                pass
    try:
        os.chmod(root_path, 0o777)
    except Exception:
        pass

def run_tv_prepare(job_id, settings, payload):
    source_path = Path(payload["source_path"]); dest_path = Path(payload["dest_path"]); files = [Path(p) for p in payload["video_files"]]
    try:
        # status may already be atomically claimed as running
        set_job_status(job_id, "running", str(dest_path))
        add_job_event(job_id, "creating destination", f"Creating {dest_path}", 5)
        dest_path.mkdir(parents=True, exist_ok=True); _chmod_chown(dest_path, settings)
        add_job_event(job_id, "copying", "Starting rsync copy for TV season video files...", 15)
        include_args = ["--include=*/"]
        for ext in VIDEO_EXTS: include_args.extend([f"--include=*.{ext}", f"--include=*.{ext.upper()}"])
        include_args.append("--exclude=*")
        cmd = ["rsync","-ah","--no-perms","--no-owner","--no-group","--info=progress2,name0"] + include_args + [str(source_path) + "/", str(dest_path) + "/"]
        rc = _run_rsync(cmd, job_id)
        if rc != 0: add_job_event(job_id, "copying", f"rsync failed with exit code {rc}", 100); finish_job(job_id, False); return
        add_job_event(job_id, "verifying", "Verifying copied video files...", 85)
        src_count, src_bytes = video_stats(files)
        dest_files = [p for p in dest_path.rglob("*") if p.is_file() and p.suffix.lower().lstrip(".") in VIDEO_EXTS]
        dest_count, dest_bytes = video_stats(dest_files)
        if dest_count < src_count or dest_bytes < src_bytes:
            add_job_event(job_id, "verifying", f"Verification failed: source {src_count} files / {human_bytes(src_bytes)}, dest {dest_count} files / {human_bytes(dest_bytes)}", 100)
            finish_job(job_id, False); return
        _apply_open_permissions_recursive(dest_path)
        save_prepared_item("tv", str(source_path), payload.get("source_rel",""), [str(p) for p in files], str(dest_path), src_bytes, dest_bytes, payload.get("detected_tags",{}), payload.get("chosen_bracket",""), settings.get("end_tag","CbHS"))
        add_job_event(job_id, "completed", f"TV prepare completed. Copied {src_count} files, {human_bytes(dest_bytes)}.", 100); finish_job(job_id, True)
    except Exception as e:
        add_job_event(job_id, "failed", f"Unhandled error: {e}", 100); finish_job(job_id, False)

def run_movie_prepare(job_id, settings, payload):
    source_file = Path(payload["source_file"]); source_path = Path(payload["source_path"]); dest_path = Path(payload["dest_path"])
    try:
        # status may already be atomically claimed as running
        set_job_status(job_id, "running", str(dest_path))
        add_job_event(job_id, "creating destination", f"Creating {dest_path}", 5)
        dest_path.mkdir(parents=True, exist_ok=True); _chmod_chown(dest_path, settings)
        add_job_event(job_id, "copying", f"Copying largest non-trailer file: {source_file.name}", 20)
        cmd = ["rsync","-ah","--no-perms","--no-owner","--no-group","--info=progress2,name0", str(source_file), str(dest_path) + "/"]
        rc = _run_rsync(cmd, job_id)
        if rc != 0: add_job_event(job_id, "copying", f"rsync failed with exit code {rc}", 100); finish_job(job_id, False); return
        add_job_event(job_id, "verifying", "Verifying copied file...", 85)
        src_bytes = source_file.stat().st_size; copied = dest_path / source_file.name; dest_bytes = copied.stat().st_size if copied.exists() else 0
        if dest_bytes < src_bytes:
            add_job_event(job_id, "verifying", f"Verification failed: source {human_bytes(src_bytes)}, dest {human_bytes(dest_bytes)}", 100)
            finish_job(job_id, False); return
        _apply_open_permissions_recursive(dest_path)
        save_prepared_item("movie", str(source_path), payload.get("source_rel",""), [str(source_file)], str(dest_path), src_bytes, dest_bytes, payload.get("detected_tags",{}), payload.get("chosen_bracket",""), settings.get("end_tag","CbHS"))
        add_job_event(job_id, "completed", f"Movie prepare completed. Copied {source_file.name}, {human_bytes(dest_bytes)}.", 100); finish_job(job_id, True)
    except Exception as e:
        add_job_event(job_id, "failed", f"Unhandled error: {e}", 100); finish_job(job_id, False)
