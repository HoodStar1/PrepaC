import os, shutil
import time
import logging
from pathlib import Path
from app.helpers import human_bytes, video_stats
from app.jobs import add_job_event, set_job_status, finish_job, register_prepare_proc, unregister_prepare_proc, get_prepare_job_status
from app.history_db import save_prepared_item
from app.subprocess_utils import run_command_with_output

LOG = logging.getLogger(__name__)

VIDEO_EXTS = ("mkv","mp4","avi","mov","m4v","ts","m2ts","wmv","mpg","mpeg","webm")

PERMISSION_MAP = {
    "legacy_open": {"dir": 0o777, "file": 0o666},
    "shared_safe": {"dir": 0o775, "file": 0o664},
    "owner_strict": {"dir": 0o750, "file": 0o640},
}


def _prepare_permissions_mode(settings):
    configured = str(settings.get("prepare_permissions_mode", "") or "").strip().lower()
    if configured in PERMISSION_MAP:
        return configured
    env_mode = str(os.environ.get("PREPAC_PREPARE_PERMISSIONS_MODE", "") or "").strip().lower()
    if env_mode in PERMISSION_MAP:
        return env_mode
    return "legacy_open"


def _permission_pair(settings):
    return PERMISSION_MAP.get(_prepare_permissions_mode(settings), PERMISSION_MAP["legacy_open"])

def _int_setting(settings, key, env_key, default_value, min_value):
    raw = None
    if isinstance(settings, dict):
        raw = settings.get(key)
    if raw is None:
        raw = os.environ.get(env_key)
    try:
        value = int(str(raw if raw is not None else default_value).strip())
    except Exception:
        value = int(default_value)
    return max(int(min_value), value)


def _run_rsync(cmd, job_id, settings=None):
    attempts = 2
    for attempt in range(1, attempts + 1):
        proc_ref = {"proc": None}
        last_emit_ts = 0.0
        last_emitted_msg = ""

        def _on_output(segment):
            nonlocal last_emit_ts, last_emitted_msg
            msg = str(segment or "").strip().rstrip("\r")
            if not msg:
                return
            now_ts = time.monotonic()
            is_progress = ("to-check=" in msg) or ("%" in msg)
            should_emit = (
                (not is_progress)
                or (msg != last_emitted_msg and (now_ts - last_emit_ts) >= 1.5)
                or ((now_ts - last_emit_ts) >= 5.0)
            )
            if should_emit:
                add_job_event(job_id, "copying", msg[:400], None)
                last_emit_ts = now_ts
                last_emitted_msg = msg

        def _should_stop(_proc):
            return get_prepare_job_status(job_id).lower() == "cancelled"

        def _on_proc_start(proc):
            proc_ref["proc"] = proc
            register_prepare_proc(job_id, proc)

        try:
            rc, _out = run_command_with_output(
                cmd,
                retries=1,
                on_output=_on_output,
                start_new_session=True,
                should_stop=_should_stop,
                on_proc_start=_on_proc_start,
                inactivity_timeout_seconds=0.0,
                runtime_timeout_seconds=0.0,
            )
        except FileNotFoundError:
            add_job_event(job_id, "copying", "rsync executable was not found in the container PATH.", 100)
            return 127
        except Exception as exc:
            add_job_event(job_id, "copying", f"rsync launch failed: {exc}", 100)
            return 125
        finally:
            unregister_prepare_proc(job_id, proc_ref.get("proc"))
        if rc != 0:
            cmd_str = " ".join(str(a) for a in cmd)
            add_job_event(job_id, "copying", f"rsync command: {cmd_str}"[:800], None)
        if rc == 0 or get_prepare_job_status(job_id).lower() == 'cancelled' or attempt >= attempts:
            return rc
        add_job_event(job_id, "copying", f"rsync attempt {attempt} failed with exit code {rc}; retrying once", None)
    return rc

def _chmod_chown(dest_path, settings):
    perm = _permission_pair(settings)
    try:
        if os.path.isdir(dest_path):
            os.chmod(dest_path, perm["dir"])
        else:
            os.chmod(dest_path, perm["file"])
    except Exception:
        pass
    try:
        shutil.chown(dest_path, user=settings.get("owner_user", ""), group=settings.get("owner_group", ""))
    except Exception:
        pass

def _apply_open_permissions_recursive(root_path):
    from app.db import load_settings
    settings = load_settings()
    perm = _permission_pair(settings)
    root_path = str(root_path)
    for current_root, dirnames, filenames in os.walk(root_path):
        for d in dirnames:
            try:
                os.chmod(os.path.join(current_root, d), perm["dir"])
            except Exception:
                pass
        for f in filenames:
            try:
                os.chmod(os.path.join(current_root, f), perm["file"])
            except Exception:
                pass
    try:
        os.chmod(root_path, perm["dir"])
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
        if not source_path.exists():
            add_job_event(job_id, "copying", f"Source path does not exist: {source_path}", 100)
            finish_job(job_id, False); return
        include_args = ["--include=*/"]
        for ext in VIDEO_EXTS: include_args.extend([f"--include=*.{ext}", f"--include=*.{ext.upper()}"])
        include_args.append("--exclude=*")
        io_timeout = _int_setting(settings, "prepare_rsync_io_timeout_seconds", "PREPAC_PREPARE_RSYNC_IO_TIMEOUT_SECONDS", 600, 60)
        cmd = [
            "rsync", "-ah", "--no-perms", "--no-owner", "--no-group",
            f"--timeout={io_timeout}",
            "--info=progress2,name0",
        ] + include_args + [str(source_path) + "/", str(dest_path) + "/"]
        add_job_event(job_id, "copying", f"Running: {' '.join(cmd)}"[:800], None)
        rc = _run_rsync(cmd, job_id, settings=settings)
        if rc != 0:
            if get_prepare_job_status(job_id).lower() == 'cancelled':
                add_job_event(job_id, 'cancelled', 'Prepare job stopped by user', None)
                return
            add_job_event(job_id, "copying", f"rsync failed with exit code {rc}", 100)
            finish_job(job_id, False)
            return
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
        if not source_file.exists():
            add_job_event(job_id, "copying", f"Source file does not exist: {source_file}", 100)
            finish_job(job_id, False); return
        io_timeout = _int_setting(settings, "prepare_rsync_io_timeout_seconds", "PREPAC_PREPARE_RSYNC_IO_TIMEOUT_SECONDS", 600, 60)
        cmd = [
            "rsync", "-ah", "--no-perms", "--no-owner", "--no-group",
            f"--timeout={io_timeout}",
            "--info=progress2,name0",
            str(source_file), str(dest_path) + "/"
        ]
        add_job_event(job_id, "copying", f"Running: {' '.join(cmd)}"[:800], None)
        rc = _run_rsync(cmd, job_id, settings=settings)
        if rc != 0:
            if get_prepare_job_status(job_id).lower() == 'cancelled':
                add_job_event(job_id, 'cancelled', 'Prepare job stopped by user', None)
                return
            add_job_event(job_id, "copying", f"rsync failed with exit code {rc}", 100)
            finish_job(job_id, False)
            return
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
