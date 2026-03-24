
import csv
import io
import os
import pathlib
import json
import threading
import signal
import atexit
import time
import requests
import fcntl
from flask import Flask, jsonify, redirect, render_template, request, url_for, flash, Response, session, stream_with_context
from urllib.parse import urlencode

from app.db import init_db, load_settings, save_settings, get_conn
from app.jobs import create_job, add_job_event, list_jobs, interrupt_running_prepare_jobs
from app.history_db import list_history, delete_prepared_by_source_path, delete_prepared_by_id
from app.clean_actions import list_clean_actions
from app.prepare_tv import search_shows, list_seasons, preview_tv
from app.prepare_movie import search_movies, preview_movie
from app.copy_engine import run_tv_prepare, run_movie_prepare
from app.plex_clean_preview import list_libraries, preview_clean, search_posters_for_prepare
from app.clean_engine import delete_candidate
from app.plex_auth import create_pin, check_pin, list_servers_for_token, save_selected_server, build_auth_url, choose_best_server_connection
from app.plex_notify import notify_after_clean
from app.posters import show_poster, movie_poster
from app.packing_core import scan_watch_folder, start_packing_job_async
from app.packing_jobs import list_packing_jobs, list_packing_history, interrupt_running_packing_jobs, get_existing_active_packing_job_id, has_outdated_or_missing_successful_packing
from app.posting_core import scan_posting_candidates, start_posting_job_async, get_posting_live_output, get_posting_live_stats
from app.posting_jobs import list_posting_jobs, list_posting_history, interrupt_running_posting_jobs, add_posting_event, get_existing_active_posting_job_id, has_outdated_or_missing_successful_posting
from app.secret_utils import SECRET_SPECS, masked_secret_value, secret_source, resolve_secret
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime
from app.version import APP_NAME, APP_VERSION, BUILD_NUMBER, FULL_VERSION, DISPLAY_VERSION, BUILD_DISPLAY

app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.secret_key = "prepac-phase7-authfix"
init_db()




@app.context_processor
def inject_version_info():
    return {
        "app_name": APP_NAME,
        "app_version": APP_VERSION,
        "build_number": BUILD_NUMBER,
        "full_version": FULL_VERSION,
        "display_version": DISPLAY_VERSION,
        "build_display": BUILD_DISPLAY,
    }

def auth_initialized(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_initialized", "false")).lower() == "true" and bool(str(settings.get("auth_username", "") or "").strip()) and bool(str(settings.get("auth_password_hash", "") or "").strip())

def auth_username(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_username", "") or "").strip()

def auth_password_hash(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_password_hash", "") or "").strip()

def auth_recovery_hash(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_recovery_hash", "") or "").strip()

def reset_token_configured():
    token = resolve_secret("auth_password_reset_token", {})
    return bool(str(token or "").strip())

def is_authenticated():
    return session.get("auth_ok") is True


def current_external_base_url():
    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}".rstrip("/")


@app.before_request
def enforce_authentication():
    endpoint = request.endpoint or ""
    allowed = {"health_page", "login_page", "setup_page", "reset_password_page", "logout_page", "static"}
    settings = load_settings()

    if endpoint in allowed:
        if endpoint == "setup_page":
            return None
        if not auth_initialized(settings) and endpoint not in {"health_page", "setup_page", "static"}:
            return redirect(url_for("setup_page"))
        return None

    if not auth_initialized(settings):
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Authentication setup required"}), 403
        return redirect(url_for("setup_page"))

    if not is_authenticated():
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Authentication required"}), 401
        return redirect(url_for("login_page", next=request.path))

APP_RUNTIME_STATE = {
    "draining": False,
    "shutdown_marked": False,
}

def mark_running_jobs_interrupted(reason="Interrupted by container shutdown", recovery=False):
    if APP_RUNTIME_STATE.get("shutdown_marked") and not recovery:
        return 0
    total = 0
    try:
        total += interrupt_running_prepare_jobs(reason=reason, recovery=recovery)
    except Exception:
        pass
    try:
        total += interrupt_running_packing_jobs(reason=reason, recovery=recovery)
    except Exception:
        pass
    try:
        total += interrupt_running_posting_jobs(reason=reason, recovery=recovery)
    except Exception:
        pass
    if not recovery:
        APP_RUNTIME_STATE["shutdown_marked"] = True
    return total

# Crash-safe recovery on startup: any stale running jobs from a previous process become interrupted.
mark_running_jobs_interrupted(recovery=True)

def begin_graceful_shutdown(reason="Container shutdown requested"):
    APP_RUNTIME_STATE["draining"] = True
    mark_running_jobs_interrupted(reason=reason, recovery=False)

def _signal_handler(signum, frame):
    begin_graceful_shutdown(reason=f"Container shutdown requested (signal {signum})")

for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _signal_handler)
    except Exception:
        pass

atexit.register(lambda: begin_graceful_shutdown(reason="Application exiting"))


@app.template_filter("prettyjson")
def prettyjson(v):
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:
            return v
    return json.dumps(v, indent=2, ensure_ascii=False)


@app.template_filter("humansize")
def humansize_filter(num_bytes):
    try:
        gb = float(num_bytes or 0) / (1024**3)
    except Exception:
        gb = 0.0
    if gb >= 1024:
        return f"{gb/1024:.2f} TB"
    return f"{gb:.2f} GB"

@app.template_filter("humanduration")
def humanduration_filter(seconds):
    try:
        total = int(round(float(seconds or 0)))
    except Exception:
        total = 0
    hrs = total // 3600
    mins = (total % 3600) // 60
    secs = total % 60
    if hrs:
        return f"{hrs}h {mins}m {secs}s"
    if mins:
        return f"{mins}m {secs}s"
    return f"{secs}s"

PREPARE_QUEUE_LOCK = threading.Lock()
PACKING_QUEUE_LOCK = threading.Lock()

HEALTH_FAILURE_STATE = {"count": 0, "reason": "", "exit_scheduled": False}

def _parse_iso(ts):
    try:
        return datetime.fromisoformat(ts) if ts else None
    except Exception:
        return None

def _latest_job_activity(job):
    times = []
    for field in ("finished_at", "started_at", "created_at"):
        dt = _parse_iso(job.get(field))
        if dt:
            times.append(dt)
    for ev in job.get("events", []) or []:
        dt = _parse_iso(ev.get("timestamp"))
        if dt:
            times.append(dt)
    return max(times) if times else None

def _evaluate_health_state():
    payload = {
        "status": "ok",
        "db": "ok",
        "running": {"prepare": 0, "packing": 0, "posting": 0},
        "stalled": [],
        "failure_count": HEALTH_FAILURE_STATE["count"],
    }

    if APP_RUNTIME_STATE.get("draining"):
        payload["status"] = "draining"
        payload["reason"] = "container is draining for graceful shutdown"
        return True, payload

    # DB probe
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        conn.close()
    except Exception as e:
        payload["status"] = "error"
        payload["db"] = "error"
        payload["reason"] = f"db probe failed: {e}"
        return False, payload

    now_dt = datetime.now()
    thresholds = {
        "prepare": 10 * 60,
        "packing": 15 * 60,
        "posting": 10 * 60,
    }

    try:
        prepare_jobs = list_jobs(5000)
        packing_jobs = list_packing_jobs(5000)
        posting_jobs = list_posting_jobs(5000)
    except Exception as e:
        payload["status"] = "error"
        payload["reason"] = f"job listing failed: {e}"
        return False, payload

    def inspect(kind, jobs, threshold_seconds):
        stalled = []
        running = [j for j in jobs if str(j.get("status", "")).lower() == "running"]
        payload["running"][kind] = len(running)
        for job in running:
            last_dt = _latest_job_activity(job)
            if not last_dt:
                continue
            age = int((now_dt - last_dt).total_seconds())
            if age > threshold_seconds:
                stalled.append({
                    "kind": kind,
                    "job": job.get("job_name") or job.get("source_path") or job.get("id"),
                    "seconds_since_activity": age,
                    "phase": job.get("phase", ""),
                    "message": job.get("message", ""),
                })
        return stalled

    payload["stalled"].extend(inspect("prepare", prepare_jobs, thresholds["prepare"]))
    payload["stalled"].extend(inspect("packing", packing_jobs, thresholds["packing"]))
    payload["stalled"].extend(inspect("posting", posting_jobs, thresholds["posting"]))

    if payload["stalled"]:
        payload["status"] = "error"
        first = payload["stalled"][0]
        payload["reason"] = f"{first['kind']} job stalled for {first['seconds_since_activity']} seconds"
        return False, payload

    return True, payload

def _schedule_unhealthy_exit():
    if HEALTH_FAILURE_STATE["exit_scheduled"]:
        return
    HEALTH_FAILURE_STATE["exit_scheduled"] = True

    def _killer():
        time.sleep(1.0)
        os._exit(1)

    threading.Thread(target=_killer, daemon=True).start()


def _prepare_running_count():
    return sum(1 for j in list_jobs(5000) if str(j.get("status","")).lower() == "running")

def _packing_running_count():
    return sum(1 for j in list_packing_jobs(5000) if str(j.get("status","")).lower() == "running")

def run_prepare_job_when_slot(job_id, media_type, settings, payload):
    while True:
        current = load_settings()
        try:
            max_jobs = max(1, int(current.get("prepare_max_concurrent_jobs", settings.get("prepare_max_concurrent_jobs", "1")) or 1))
        except Exception:
            max_jobs = 1
        with PREPARE_QUEUE_LOCK:
            if _prepare_running_count() < max_jobs:
                break
        add_job_event(job_id, "queued", f"Waiting for prepare slot ({max_jobs} max concurrent jobs).", 0)
        time.sleep(1)
    if media_type == "tv":
        run_tv_prepare(job_id, load_settings(), payload)
    else:
        run_movie_prepare(job_id, load_settings(), payload)

def run_packing_job_when_slot(source_path, settings, existing_job_id=None):
    while True:
        current = load_settings()
        try:
            max_jobs = max(1, int(current.get("packing_max_concurrent_jobs", settings.get("packing_max_concurrent_jobs", "1")) or 1))
        except Exception:
            max_jobs = 1
        with PACKING_QUEUE_LOCK:
            if _packing_running_count() < max_jobs:
                break
        if existing_job_id:
            try:
                from packing_jobs import add_packing_event
                add_packing_event(existing_job_id, "queued", f"Waiting for packing slot ({max_jobs} max concurrent jobs).", 0)
            except Exception:
                pass
        time.sleep(1)
    from packing_core import run_packing_job
    return run_packing_job(existing_job_id, source_path, load_settings())

def _job_duration_seconds(started_at, finished_at):
    if not started_at or not finished_at:
        return 0
    try:
        return max(0, int((datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)).total_seconds()))
    except Exception:
        return 0

def enrich_prepare_history_rows(history_rows, jobs):
    done_jobs = [j for j in jobs if str(j.get("status","")).lower() == "done"]
    for h in history_rows:
        match = next((j for j in done_jobs if j.get("source_path") == h.get("source_path") and j.get("dest_path") == h.get("dest_path")), None)
        if match:
            h["duration_seconds"] = _job_duration_seconds(match.get("started_at"), match.get("finished_at"))
        else:
            h["duration_seconds"] = 0
    return history_rows

def enrich_packing_history_rows(job_rows):
    for j in job_rows:
        j["rar_size_bytes"] = int(j.get("rar_size_bytes") or 0)
        j["par2_size_bytes"] = int(j.get("par2_size_bytes") or 0)
        j["rar_parts_actual"] = int(j.get("rar_parts_actual") or 0)
        j["rar_time_seconds"] = int(j.get("rar_time_seconds") or 0)
        j["par2_time_seconds"] = int(j.get("par2_time_seconds") or 0)
        j["total_time_seconds"] = _job_duration_seconds(j.get("started_at"), j.get("finished_at"))
    return job_rows

def summarize_clean_logs(logs):
    summary = {
        "total_actions": len(logs),
        "dry_runs": 0,
        "real_runs": 0,
        "successes": 0,
        "failures": 0,
        "bytes_total": 0,
        "bytes_dry_run": 0,
        "bytes_real": 0,
        "recycle_actions": 0,
    }
    for l in logs:
        is_dry = str(l.get("dry_run", "")).lower() == "true"
        is_success = str(l.get("success", "")).lower() == "true"
        size = int(l.get("size_bytes", 0) or 0)
        msg = str(l.get("message","")).lower()
        summary["bytes_total"] += size
        if "recycle" in msg:
            summary["recycle_actions"] += 1
        if is_dry:
            summary["dry_runs"] += 1
            summary["bytes_dry_run"] += size
        else:
            summary["real_runs"] += 1
            summary["bytes_real"] += size
        if is_success:
            summary["successes"] += 1
        else:
            summary["failures"] += 1
    return summary


def summarize_prepare_stats(history, jobs):
    completed_jobs = [j for j in jobs if str(j.get("status","")).lower() == "done"]
    summary = {
        "total_prepared_items": len(history),
        "total_prepare_jobs": len(completed_jobs),
        "tv_items": 0,
        "movie_items": 0,
        "source_bytes_total": 0,
        "dest_bytes_total": 0,
    }
    for h in history:
        if h.get("media_type") == "tv":
            summary["tv_items"] += 1
        elif h.get("media_type") == "movie":
            summary["movie_items"] += 1
        summary["source_bytes_total"] += int(h.get("source_bytes", 0) or 0)
        summary["dest_bytes_total"] += int(h.get("dest_bytes", 0) or 0)
    return summary




def summarize_packing_stats(packing_jobs):
    completed = [j for j in packing_jobs if str(j.get("status","")).lower() == "done"]
    total_bytes = sum(int(j.get("size_bytes", 0) or 0) for j in completed)
    durations = []
    largest = 0
    for j in completed:
        largest = max(largest, int(j.get("size_bytes", 0) or 0))
        s = j.get("started_at")
        f = j.get("finished_at")
        if s and f:
            try:

                durations.append((datetime.fromisoformat(f) - datetime.fromisoformat(s)).total_seconds())
            except Exception:
                pass
    avg_seconds = int(sum(durations)/len(durations)) if durations else 0
    return {
        "total_jobs": len(completed),
        "total_bytes": total_bytes,
        "largest_bytes": largest,
        "avg_seconds": avg_seconds,
    }

def summarize_posting_stats(posting_jobs):
    completed = [j for j in posting_jobs if str(j.get("status","")).lower() == "done"]
    total_bytes = sum(int(j.get("size_bytes", 0) or 0) for j in completed)
    durations = []
    largest = 0
    provider1_jobs = 0
    provider2_jobs = 0
    for j in completed:
        largest = max(largest, int(j.get("size_bytes", 0) or 0))
        if str(j.get("provider_used","")) == "provider1":
            provider1_jobs += 1
        if str(j.get("provider_used","")) == "provider2":
            provider2_jobs += 1
        s = j.get("started_at")
        f = j.get("finished_at")
        if s and f:
            try:

                durations.append((datetime.fromisoformat(f) - datetime.fromisoformat(s)).total_seconds())
            except Exception:
                pass
    avg_seconds = int(sum(durations)/len(durations)) if durations else 0
    return {
        "total_jobs": len(completed),
        "total_bytes": total_bytes,
        "largest_bytes": largest,
        "avg_seconds": avg_seconds,
        "provider1_jobs": provider1_jobs,
        "provider2_jobs": provider2_jobs,
    }



def parse_posting_log_stats(log_path):
    import re
    from pathlib import Path

    p = pathlib.Path(log_path)
    stats = {"transfer_rate": "", "percent_transferred": "", "eta": ""}
    if not p.exists():
        return stats
    text = p.read_text(encoding="utf-8", errors="replace")
    clean = re.sub(r"\x1b\[[0-9;]*m", "", text)

    total_articles = 0
    total_gb = 0.0
    m = re.search(r"Uploading\s+(\d+)\s+article\(s\)\s+from\s+(\d+)\s+file\(s\)\s+totalling\s+([0-9.]+)\s+([KMG]iB)", clean, re.I)
    if m:
        total_articles = int(m.group(1))
        total_value = float(m.group(3))
        unit = m.group(4).lower()
        if unit.startswith("mi"):
            total_gb = total_value / 1024.0
        elif unit.startswith("ki"):
            total_gb = total_value / (1024.0 * 1024.0)
        else:
            total_gb = total_value

    progress_lines = re.findall(
        r"\[(.*?)\]\[INFO\]\s+Article posting progress:\s+(\d+)\s+read,\s+(\d+)\s+posted(?:,\s+(\d+)\s+checked)?",
        clean,
        re.I
    )
    if progress_lines:
        last_ts, _last_read, last_posted, last_checked = progress_lines[-1]
        last_posted = int(last_posted)
        posted_pct = ((last_posted / max(1, total_articles)) * 100.0) if total_articles else 0.0
        stats["percent_transferred"] = f"{posted_pct:.1f}% of {max(total_gb,0.01):.2f} GB"

        if len(progress_lines) >= 2:
            prev_ts, _prev_read, prev_posted, _prev_checked = progress_lines[-2]
            prev_posted = int(prev_posted)
            try:
                t1 = datetime.strptime(prev_ts, "%Y-%m-%d %H:%M:%S.%f")
                t2 = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S.%f")
                seconds = max(0.001, (t2 - t1).total_seconds())
                article_rate = max(0.0, (last_posted - prev_posted) / seconds)
                if total_articles > 0 and total_gb > 0:
                    mib_total = total_gb * 1024.0
                    mib_per_article = mib_total / total_articles
                    mib_per_sec = article_rate * mib_per_article
                    stats["transfer_rate"] = f"{mib_per_sec:.2f} MiB/s" if mib_per_sec > 0 else "calculating..."
                    remaining_articles = max(0, total_articles - last_posted)
                    if article_rate > 0 and remaining_articles > 0:
                        eta_seconds = int(round(remaining_articles / article_rate))
                        hrs = eta_seconds // 3600
                        mins = (eta_seconds % 3600) // 60
                        secs = eta_seconds % 60
                        stats["eta"] = f"{hrs}:{mins:02d}:{secs:02d}" if hrs > 0 else f"{mins}:{secs:02d}"
                    else:
                        stats["eta"] = "calculating..."
            except Exception:
                pass
        else:
            stats["transfer_rate"] = "calculating..."
            stats["eta"] = "calculating..."
        return stats
    return stats
def summarize_running_jobs(prepare_jobs, packing_jobs, posting_jobs):
    running = []
    for j in prepare_jobs:
        if str(j.get("status","")).lower() in ("queued","running"):
            running.append({"kind":"Prepare","title":j.get("source_path",""),"phase":j.get("phase",""),"percent":j.get("percent"),"message":j.get("message","")})
    for j in packing_jobs:
        if str(j.get("status","")).lower() in ("queued","running"):
            running.append({"kind":"Packing","title":j.get("job_name",""),"phase":j.get("phase",""),"percent":j.get("percent"),"message":j.get("message","")})
    for j in posting_jobs:
        if str(j.get("status","")).lower() in ("queued","running"):
            running.append({"kind":"Posting","title":j.get("job_name",""),"phase":j.get("phase",""),"percent":j.get("percent"),"message":j.get("message","")})
    return running

def _tail_text_file(path_str, max_lines=120):
    p = pathlib.Path(path_str)
    if not p.exists():
        return ""
    try:
        with p.open("r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return "".join(lines[-max_lines:])
    except Exception:
        return ""

def build_recent_actions(history, clean_logs, packing_jobs, posting_jobs, limit=10):
    items = []
    for h in history:
        items.append({
            "kind": "prepare",
            "time": h.get("created_at", ""),
            "title": h.get("source_path", ""),
            "media_type": h.get("media_type", ""),
            "status": "completed",
            "details": h.get("dest_path", ""),
        })
    for c in clean_logs:
        items.append({
            "kind": "clean",
            "time": c.get("created_at", ""),
            "title": c.get("target_path", ""),
            "media_type": c.get("media_type", ""),
            "status": c.get("message", ""),
            "details": c.get("reason", ""),
        })
    for p in packing_jobs:
        items.append({
            "kind": "packing",
            "time": p.get("finished_at") or p.get("started_at") or p.get("created_at", ""),
            "title": p.get("source_path", ""),
            "media_type": "packing",
            "status": p.get("status",""),
            "details": p.get("phase",""),
        })
    for p in posting_jobs:
        items.append({
            "kind": "posting",
            "time": p.get("finished_at") or p.get("started_at") or p.get("created_at", ""),
            "title": p.get("job_name", ""),
            "media_type": "posting",
            "status": p.get("status",""),
            "details": p.get("provider_used",""),
        })
    items.sort(key=lambda x: x.get("time",""), reverse=True)
    return items[:limit]






def _sse_json(payload):
    return "data: " + json.dumps(payload) + "\n\n"

def _event_stream(generator_fn):
    @stream_with_context
    def generate():
        last = None
        while True:
            try:
                payload = generator_fn()
            except Exception as e:
                payload = {"ok": False, "error": str(e)}
            data = _sse_json(payload)
            if data != last:
                yield data
                last = data
            time.sleep(1)
    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

def _dashboard_running_payload():
    all_jobs = list_jobs(5000)
    all_packing_jobs = list_packing_jobs(5000)
    all_posting_jobs = list_posting_jobs(5000)
    return {"ok": True, "running": summarize_running_jobs(all_jobs, all_packing_jobs, all_posting_jobs)}

def _jobs_payload():
    return {"jobs": list_jobs(50)}

def _packing_jobs_payload():
    jobs = list_packing_jobs(200)
    for j in jobs:
        if j.get("events"):
            latest = j["events"][0]
            j["message"] = latest.get("message", j.get("message",""))
            j["phase"] = latest.get("phase", j.get("phase",""))
            j["percent"] = latest.get("percent", j.get("percent"))
    return {"ok": True, "jobs": jobs}

def _packing_completed_payload():
    jobs = enrich_packing_history_rows(list_packing_history(50))
    jobs = [j for j in jobs if str(j.get("status","")).lower() == "done"][:10]
    return {"ok": True, "jobs": jobs}

def _posting_jobs_payload():
    jobs = list_posting_jobs(200)
    for j in jobs:
        live = get_posting_live_stats(int(j.get("id", 0)))
        if any(live.values()):
            j["runtime_stats"] = live
        else:
            posted_root = j.get("posted_root") or f"/media/dest/_posted/{j.get('job_name', '')}"
            log_path = pathlib.Path(posted_root) / "posting.log"
            j["runtime_stats"] = parse_posting_log_stats(str(log_path))
        if j.get("events"):
            latest = j["events"][0]
            j["message"] = latest.get("message", j.get("message",""))
            j["phase"] = latest.get("phase", j.get("phase",""))
            j["percent"] = latest.get("percent", j.get("percent"))
    return {"ok": True, "jobs": jobs}



@app.route("/api/version")
def api_version():
    return jsonify({
        "ok": True,
        "app_name": APP_NAME,
        "app_version": APP_VERSION,
        "build_number": BUILD_NUMBER,
        "build_display": BUILD_DISPLAY,
        "full_version": FULL_VERSION,
        "display_version": DISPLAY_VERSION,
    })

@app.route("/health")
def health_page():
    healthy, payload = _evaluate_health_state()
    if healthy:
        HEALTH_FAILURE_STATE["count"] = 0
        HEALTH_FAILURE_STATE["reason"] = ""
        HEALTH_FAILURE_STATE["exit_scheduled"] = False
        payload["failure_count"] = 0
        return jsonify(payload), 200

    HEALTH_FAILURE_STATE["count"] += 1
    HEALTH_FAILURE_STATE["reason"] = payload.get("reason", "unhealthy")
    payload["failure_count"] = HEALTH_FAILURE_STATE["count"]
    if HEALTH_FAILURE_STATE["count"] >= 3:
        payload["action"] = "terminating for docker restart"
        _schedule_unhealthy_exit()
    return jsonify(payload), 503

def workflow_auto_chain_enabled(settings=None):
    settings = settings or load_settings()
    return str(settings.get("workflow_auto_chain_enabled", "false")).lower() == "true"

def _prepare_has_auto_chain_event(job):
    return any((ev.get("phase") == "auto_chain") for ev in (job.get("events") or []))

def _packing_has_auto_chain_event(job):
    return any((ev.get("phase") == "auto_chain") for ev in (job.get("events") or []))

def _has_any_packing_job_for_source(source_path, prepared_finished_at=""):
    source_path = str(source_path or "")
    if get_existing_active_packing_job_id(source_path):
        return True
    return not has_outdated_or_missing_successful_packing(source_path, prepared_finished_at)

def _has_any_posting_job_for_packed_root(packed_root, packed_finished_at=""):
    packed_root = str(packed_root or "")
    if get_existing_active_posting_job_id(packed_root):
        return True
    return not has_outdated_or_missing_successful_posting(packed_root, packed_finished_at)

def process_auto_chain_once():
    if APP_RUNTIME_STATE.get("draining"):
        return
    settings = load_settings()
    if not workflow_auto_chain_enabled(settings):
        return

    # Prepare -> Packing
    prepare_jobs = list_jobs(5000)
    for job in prepare_jobs:
        if str(job.get("status", "")).lower() != "done":
            continue
        if _prepare_has_auto_chain_event(job):
            continue
        dest_path = str(job.get("dest_path", "") or "").strip()
        if not dest_path:
            continue
        prepared_finished_at = str(job.get("finished_at") or job.get("created_at") or "")
        if _has_any_packing_job_for_source(dest_path, prepared_finished_at):
            add_job_event(job["id"], "auto_chain", "Auto-chain: packing already exists for this prepared job", 100)
            continue
        start_packing_job_async(dest_path, settings)
        add_job_event(job["id"], "auto_chain", "Auto-chain: packing queued for this prepared job", 100)

    # Packing -> Posting
    packing_jobs = list_packing_history(5000)
    for job in packing_jobs:
        if str(job.get("status", "")).lower() != "done":
            continue
        if _packing_has_auto_chain_event(job):
            continue
        packed_root = str(job.get("output_root", "") or "").strip()
        if not packed_root:
            continue
        packed_finished_at = str(job.get("finished_at") or job.get("created_at") or "")
        if _has_any_posting_job_for_packed_root(packed_root, packed_finished_at):
            add_packing_event(job["id"], "auto_chain", "Auto-chain: posting already exists for this packed job", 100)
            continue
        start_posting_job_async(packed_root, settings)
        add_packing_event(job["id"], "auto_chain", "Auto-chain: posting queued for this packed job", 100)

def auto_chain_loop():
    while True:
        try:
            process_auto_chain_once()
        except Exception:
            pass
        time.sleep(5)


AUTO_CHAIN_LOCK_FILE = "/config/prepac_auto_chain.lock"
AUTO_CHAIN_LOCK_HANDLE = None

def start_auto_chain_thread_once():
    global AUTO_CHAIN_LOCK_HANDLE
    if AUTO_CHAIN_LOCK_HANDLE is not None:
        return
    try:
        Path("/config").mkdir(parents=True, exist_ok=True)
        AUTO_CHAIN_LOCK_HANDLE = open(AUTO_CHAIN_LOCK_FILE, "w")
        fcntl.flock(AUTO_CHAIN_LOCK_HANDLE.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        AUTO_CHAIN_LOCK_HANDLE = None
        return
    threading.Thread(target=auto_chain_loop, daemon=True).start()

def reject_if_draining():
    if APP_RUNTIME_STATE.get("draining"):
        return jsonify({"ok": False, "error": "PrepaC is draining for graceful shutdown. New jobs are temporarily blocked."}), 503
    return None



@app.route("/setup", methods=["GET", "POST"])
def setup_page():
    settings = load_settings()
    if auth_initialized(settings):
        return redirect(url_for("login_page"))
    if request.method == "POST":
        username = (request.form.get("username", "") or "").strip()
        password = request.form.get("password", "") or ""
        confirm = request.form.get("confirm_password", "") or ""
        recovery = request.form.get("recovery_secret", "") or ""
        recovery_confirm = request.form.get("confirm_recovery_secret", "") or ""

        if not username:
            flash("Username is required.", "error")
        elif password != confirm:
            flash("Passwords do not match.", "error")
        elif len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
        elif recovery != recovery_confirm:
            flash("Recovery secrets do not match.", "error")
        elif len(recovery) < 8:
            flash("Recovery secret must be at least 8 characters.", "error")
        else:
            data = dict(settings)
            data["auth_username"] = username
            data["auth_password_hash"] = generate_password_hash(password)
            data["auth_recovery_hash"] = generate_password_hash(recovery)
            data["auth_initialized"] = "true"
            save_settings(data)
            flash("Admin account created. Please sign in.", "success")
            return redirect(url_for("login_page"))

    return render_template("setup.html")

start_auto_chain_thread_once()

@app.route("/login", methods=["GET", "POST"])
def login_page():
    settings = load_settings()
    if not auth_initialized(settings):
        return redirect(url_for("setup_page"))
    next_url = request.values.get("next") or url_for("dashboard")
    if request.method == "POST":
        username = (request.form.get("username", "") or "").strip()
        password = request.form.get("password", "") or ""
        if username == auth_username(settings) and check_password_hash(auth_password_hash(settings), password):
            session["auth_ok"] = True
            session["auth_user"] = username
            flash("Logged in successfully.", "success")
            return redirect(next_url)
        flash("Invalid username or password.", "error")
    return render_template("login.html", next_url=next_url)

@app.route("/reset-password", methods=["GET", "POST"])
def reset_password_page():
    settings = load_settings()
    if request.method == "POST":
        username = (request.form.get("username", "") or "").strip()
        recovery_secret = request.form.get("recovery_secret", "") or ""
        new_password = request.form.get("new_password", "") or ""
        confirm = request.form.get("confirm_password", "") or ""

        if username != auth_username(settings):
            flash("Invalid username.", "error")
        elif not auth_recovery_hash(settings):
            flash("No recovery secret is configured for this installation.", "error")
        elif not check_password_hash(auth_recovery_hash(settings), recovery_secret):
            flash("Invalid recovery secret.", "error")
        elif new_password != confirm:
            flash("Passwords do not match.", "error")
        elif len(new_password) < 8:
            flash("Password must be at least 8 characters.", "error")
        else:
            data = dict(settings)
            data["auth_password_hash"] = generate_password_hash(new_password)
            save_settings(data)
            flash("Password reset successful. Please sign in.", "success")
            return redirect(url_for("login_page"))
    return render_template("reset_password.html", username_value=auth_username(settings))

@app.route("/logout")
def logout_page():
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("login_page"))


@app.route("/change-recovery-secret", methods=["GET", "POST"])
def change_recovery_secret_page():
    settings = load_settings()
    if not auth_initialized(settings):
        return redirect(url_for("setup_page"))
    if not is_authenticated():
        return redirect(url_for("login_page", next=url_for("change_recovery_secret_page")))
    if request.method == "POST":
        current_password = request.form.get("current_password", "") or ""
        new_secret = request.form.get("new_recovery_secret", "") or ""
        confirm_secret = request.form.get("confirm_recovery_secret", "") or ""

        if not check_password_hash(auth_password_hash(settings), current_password):
            flash("Current password is incorrect.", "error")
        elif new_secret != confirm_secret:
            flash("Recovery secrets do not match.", "error")
        elif len(new_secret) < 8:
            flash("Recovery secret must be at least 8 characters.", "error")
        else:
            data = dict(settings)
            data["auth_recovery_hash"] = generate_password_hash(new_secret)
            save_settings(data)
            flash("Recovery secret updated successfully.", "success")
            return redirect(url_for("settings_page"))
    return render_template("change_recovery_secret.html")

@app.route("/")
def dashboard():
    settings = load_settings()
    all_jobs = list_jobs(5000)
    all_history = list_history(5000)
    all_clean_logs = list_clean_actions(5000)
    all_packing_jobs = list_packing_jobs(5000)
    all_posting_jobs = list_posting_jobs(5000)
    clean_summary = summarize_clean_logs(all_clean_logs)
    prepare_summary = summarize_prepare_stats(all_history, all_jobs)
    packing_summary = summarize_packing_stats(all_packing_jobs)
    posting_summary = summarize_posting_stats(all_posting_jobs)
    current_running = summarize_running_jobs(all_jobs, all_packing_jobs, all_posting_jobs)
    recent_actions = build_recent_actions(all_history, all_clean_logs, all_packing_jobs, all_posting_jobs, 10)
    return render_template(
        "dashboard.html",
        settings=settings,
        jobs=all_jobs[:10],
        history=all_history[:10],
        clean_logs=all_clean_logs[:10],
        clean_summary=clean_summary,
        prepare_summary=prepare_summary,
        packing_summary=packing_summary,
        posting_summary=posting_summary,
        current_running=current_running,
        recent_actions=recent_actions,
    )


@app.route("/api/dashboard/running")
def api_dashboard_running():
    return jsonify(_dashboard_running_payload())

@app.route("/api/dashboard/running/stream")
def api_dashboard_running_stream():
    return _event_stream(_dashboard_running_payload)


HELP_TOPICS = [
    {"slug": "getting-started", "title": "Getting Started"},
    {"slug": "prepare", "title": "Prepare"},
    {"slug": "packing", "title": "Packing"},
    {"slug": "posting", "title": "Posting"},
    {"slug": "clean", "title": "Clean"},
    {"slug": "settings", "title": "Settings"},
]

@app.route("/help")
def help_page():
    topic = request.args.get("topic", "getting-started")
    valid = {t["slug"] for t in HELP_TOPICS}
    if topic not in valid:
        topic = "getting-started"
    return render_template("help.html", topics=HELP_TOPICS, active_topic=topic, settings=load_settings())
@app.route("/settings")
def settings_page():
    settings = load_settings()
    display_settings = dict(settings)
    for key in SECRET_SPECS.keys():
        display_settings[key] = masked_secret_value(key, settings)
        display_settings[key + "_source"] = secret_source(key, settings)
    return render_template("settings.html", settings=display_settings)

@app.route("/plex")
def plex_page():
    return redirect(url_for("settings_page"))

@app.route("/clean")
def clean_page():
    return render_template("clean.html", settings=load_settings())

@app.route("/clean/logs")
def clean_logs_page():
    return render_template("clean_logs.html", logs=list_clean_actions(500))

@app.route("/api/settings/save", methods=["POST"])
def api_settings_save():
    current = load_settings()
    data = dict(current)
    for k in ["tv_root","movie_root","youtube_root","dest_root","end_tag","prepare_max_concurrent_jobs","packing_max_concurrent_jobs","recycle_bin_root","plex_url","plex_token","plex_tv_library","plex_movie_library","plex_youtube_library","packing_watch_root","packing_output_root","packing_stability_delay","packing_password_prefix","packing_password_length","packing_par2_threads","packing_par2_memory_mb","packing_par2_block_size","packing_name_length","packing_name_fixed_tag","packing_name_fixed_pos","packing_thumbnail_host","packing_freeimage_api_key","posting_posted_root","posting_nzb_root","posting_randomizer_file","posting_article_size","posting_yenc_line_size","posting_retries","posting_retry_delay","posting_comment","posting_provider2_max_gb_when_busy","posting_provider1_host","posting_provider1_port","posting_provider1_username","posting_provider1_password","posting_provider1_connections","posting_provider1_max_connections","posting_provider2_host","posting_provider2_port","posting_provider2_username","posting_provider2_password","posting_provider2_connections","posting_provider2_max_connections","auth_username"]:
        if k in request.form:
            incoming = request.form.get(k, current.get(k, "")).strip()
            if k in SECRET_SPECS and incoming.startswith("********"):
                incoming = current.get(k, "")
            data[k] = incoming
    data["clean_dry_run"] = "true" if request.form.get("clean_dry_run") else "false"
    data["clean_use_recycle_bin"] = "true" if request.form.get("clean_use_recycle_bin") else "false"
    data["packing_delete_source_after_success"] = "true" if request.form.get("packing_delete_source_after_success") else "false"
    data["packing_header_encrypt"] = "true" if request.form.get("packing_header_encrypt") else "false"
    data["packing_auto_volume"] = "true" if request.form.get("packing_auto_volume") else "false"
    data["packing_auto_par2"] = "true" if request.form.get("packing_auto_par2") else "false"
    data["posting_embed_password_in_nzb"] = "true" if request.form.get("posting_embed_password_in_nzb") else "false"
    data["posting_post_check"] = "true" if request.form.get("posting_post_check") else "false"
    data["posting_provider1_enabled"] = "true" if request.form.get("posting_provider1_enabled") else "false"
    data["posting_provider1_ssl"] = "true" if request.form.get("posting_provider1_ssl") else "false"
    data["posting_provider2_enabled"] = "true" if request.form.get("posting_provider2_enabled") else "false"
    data["posting_provider2_ssl"] = "true" if request.form.get("posting_provider2_ssl") else "false"
    data["workflow_auto_chain_enabled"] = "true" if request.form.get("workflow_auto_chain_enabled") else "false"
    save_settings(data)
    flash("Settings saved.", "success")
    return redirect(url_for("settings_page"))

@app.route("/api/plex/save", methods=["POST"])
def api_plex_save():
    current = load_settings()
    data = dict(current)
    data["plex_url"] = request.form.get("plex_url","").strip()
    data["plex_token"] = request.form.get("plex_token","").strip()
    data["plex_tv_library"] = request.form.get("plex_tv_library","").strip()
    data["plex_movie_library"] = request.form.get("plex_movie_library","").strip()
    data["plex_youtube_library"] = request.form.get("plex_youtube_library","").strip()
    save_settings(data)
    flash("Plex settings saved.", "success")
    return redirect(url_for("plex_page"))


@app.route("/plex/signin")
def plex_signin():
    s = load_settings()
    client_id = s.get("plex_client_id", "prepac-local-client")
    product = s.get("plex_product_name", "PrepaC")
    pin = create_pin(client_id, product)
    pin_id = str(pin.get("id"))
    session["plex_pending_pin_id"] = pin_id
    forward_url = f"{current_external_base_url()}{url_for('plex_callback')}" + "?" + urlencode({"pin_id": pin_id})
    auth_url = build_auth_url(client_id, product, pin["code"], forward_url)
    return redirect(auth_url)

@app.route("/plex/callback")
def plex_callback():
    s = load_settings()
    client_id = s.get("plex_client_id", "prepac-local-client")
    product = s.get("plex_product_name", "PrepaC")
    pin_id = request.args.get("pin_id", "") or session.get("plex_pending_pin_id", "")
    if not pin_id:
        flash("Plex sign-in could not be completed because the PIN information was missing.", "error")
        return redirect(url_for("settings_page"))

    try:
        result = check_pin(pin_id, client_id, product)
    except Exception as e:
        flash(f"Plex sign-in failed: {e}", "error")
        return redirect(url_for("settings_page"))

    if not result.get("authorized"):
        flash("Plex sign-in did not return an authorized token.", "error")
        return redirect(url_for("settings_page"))

    token = result.get("token", "")
    chosen_url = ""
    try:
        servers = list_servers_for_token(token, client_id, product)
        chosen_url = choose_best_server_connection(servers)
    except Exception:
        servers = []

    settings = load_settings()
    settings["plex_token"] = token
    if chosen_url:
        settings["plex_url"] = chosen_url
    save_settings(settings)
    session.pop("plex_pending_pin_id", None)

    if chosen_url:
        flash(f"Plex sign-in completed. Auto-selected server: {chosen_url}", "success")
    else:
        flash("Plex sign-in completed. Token saved, but no server URL was auto-selected.", "success")
    return redirect(url_for("settings_page"))

@app.route("/api/plex/pin/start", methods=["POST"])
def api_plex_pin_start():
    return jsonify({"ok": True, "redirect_url": url_for("plex_signin"), "full_redirect_url": f"{current_external_base_url()}{url_for('plex_signin')}"})

@app.route("/api/plex/pin/check")
def api_plex_pin_check():
    return jsonify({"ok": False, "error": "PIN polling is no longer required. Use the Sign in with Plex flow."}), 410

@app.route("/api/plex/servers")
def api_plex_servers():
    s = load_settings()
    token = s.get("plex_token", "").strip()
    if not token:
        return jsonify({"ok": False, "error": "No Plex token saved yet."}), 400
    client_id = s.get("plex_client_id", "prepac-local-client")
    product = s.get("plex_product_name", "PrepaC")
    servers = list_servers_for_token(token, client_id, product)
    return jsonify({"ok": True, "servers": servers})

@app.route("/api/plex/server/select", methods=["POST"])
def api_plex_server_select():
    data = request.get_json(force=True)
    server_url = (data.get("server_url") or "").strip()
    if not server_url:
        return jsonify({"ok": False, "error": "server_url required"}), 400
    s = save_selected_server(server_url)
    return jsonify({"ok": True, "plex_url": s.get("plex_url"), "token_saved": bool(s.get("plex_token"))})



@app.route("/api/local_image")
def api_local_image():
    path = (request.args.get("path") or "").strip()
    if not path:
        return ("", 404)
    p = pathlib.Path(path)
    if not p.exists() or not p.is_file():
        return ("", 404)
    ext = p.suffix.lower()
    mime = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
    }.get(ext, "application/octet-stream")
    try:
        return Response(p.read_bytes(), mimetype=mime, headers={"Cache-Control": "no-cache"})
    except Exception:
        return ("", 404)

@app.route("/api/plex/image")
def api_plex_image():
    settings = load_settings()
    plex_url = (settings.get("plex_url") or "").strip()
    plex_token = (settings.get("plex_token") or "").strip()
    path = (request.args.get("path") or "").strip()
    if not plex_url or not plex_token or not path:
        return ("", 404)

    headers = {"X-Plex-Token": plex_token}
    photo_url = f"{plex_url.rstrip('/')}/photo/:/transcode"
    params = {
        "url": path,
        "width": request.args.get("width", "400"),
        "height": request.args.get("height", "600"),
        "minSize": "1",
        "upscale": "1",
    }
    try:
        r = requests.get(photo_url, headers=headers, params=params, timeout=60)
        if r.status_code == 200 and r.content:
            return Response(r.content, mimetype=r.headers.get("Content-Type", "image/jpeg"), headers={"Cache-Control":"no-cache"})
    except Exception:
        pass

    try:
        direct = requests.get(f"{plex_url.rstrip('/')}{path}", headers=headers, timeout=60)
        if direct.status_code == 200 and direct.content:
            return Response(direct.content, mimetype=direct.headers.get("Content-Type", "image/jpeg"), headers={"Cache-Control":"no-cache"})
    except Exception:
        pass

    return ("", 404)

@app.route("/api/clean/reset_prepared", methods=["POST"])
def api_clean_reset_prepared():
    data = request.get_json(force=True)
    prepared_item_id = data.get("prepared_item_id")
    source_path = (data.get("source_path") or "").strip()

    removed = 0
    removed_by = None

    if prepared_item_id not in (None, ""):
        try:
            removed = delete_prepared_by_id(int(prepared_item_id))
            if removed:
                removed_by = "id"
        except Exception:
            removed = 0

    if removed == 0 and source_path:
        removed = delete_prepared_by_source_path(source_path)
        if removed:
            removed_by = "source_path"

    if removed == 0:
        return jsonify({
            "ok": False,
            "error": "No prepared record was removed",
            "prepared_item_id": prepared_item_id,
            "source_path": source_path,
        }), 404

    return jsonify({
        "ok": True,
        "removed": removed,
        "removed_by": removed_by,
        "prepared_item_id": prepared_item_id,
        "source_path": source_path,
    })

@app.route("/prepare")
def prepare_page():
    return render_template("prepare.html", settings=load_settings())

@app.route("/prepare/tv")
def prepare_tv_page():
    return redirect(url_for("prepare_page"))

@app.route("/prepare/movie")
def prepare_movie_page():
    return redirect(url_for("prepare_page"))

@app.route("/jobs")
def jobs_page():
    return render_template("jobs.html")



@app.route("/packing")
def packing_page():
    return render_template("packing.html", settings=load_settings())

@app.route("/api/packing/scan", methods=["POST"])
def api_packing_scan():
    settings = load_settings()
    return jsonify({"ok": True, "results": scan_watch_folder(settings)})

@app.route("/api/packing/start", methods=["POST"])
def api_packing_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    data = request.get_json(force=True)
    settings = load_settings()
    started = []
    for source_path in data.get("source_paths", []):
        started.append(start_packing_job_async(source_path, settings))
    return jsonify({"ok": True, "job_ids": started})

@app.route("/api/packing/jobs")
def api_packing_jobs():
    return jsonify(_packing_jobs_payload())

@app.route("/api/packing/jobs/stream")
def api_packing_jobs_stream():
    return _event_stream(_packing_jobs_payload)



@app.route("/api/packing/completed")
def api_packing_completed():
    return jsonify(_packing_completed_payload())

@app.route("/api/packing/completed/stream")
def api_packing_completed_stream():
    return _event_stream(_packing_completed_payload)

@app.route("/posting")
def posting_page():
    return render_template("posting.html", settings=load_settings())

@app.route("/api/posting/scan", methods=["POST"])
def api_posting_scan():
    settings = load_settings()
    return jsonify({"ok": True, "results": scan_posting_candidates(settings)})

@app.route("/api/posting/start", methods=["POST"])
def api_posting_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    data = request.get_json(force=True)
    settings = load_settings()
    started = []
    for packed_root in data.get("packed_roots", []):
        started.append(start_posting_job_async(packed_root, settings))
    return jsonify({"ok": True, "job_ids": started})

@app.route("/api/posting/jobs")
def api_posting_jobs():
    return jsonify(_posting_jobs_payload())

@app.route("/api/posting/jobs/stream")
def api_posting_jobs_stream():
    return _event_stream(_posting_jobs_payload)


@app.route("/api/posting/output/<int:job_id>")
def api_posting_output(job_id):
    raw_output = get_posting_live_output(job_id)
    stats = get_posting_live_stats(job_id)
    if raw_output:
        return jsonify({"ok": True, "raw_output": raw_output, "stats": stats, "source": "memory"})
    jobs = list_posting_jobs(5000)
    job = next((j for j in jobs if int(j.get("id", 0)) == int(job_id)), None)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    posted_root = job.get("posted_root") or f"/media/dest/_posted/{job.get('job_name', '')}"
    log_path = pathlib.Path(posted_root) / "posting.log"
    return jsonify({"ok": True, "raw_output": _tail_text_file(str(log_path), 200), "stats": parse_posting_log_stats(str(log_path)), "source": "log"})

@app.route("/clean/result")
def clean_result_page():
    return render_template("clean_result.html")

@app.route("/history")
def history_page():
    return render_template("history_index.html")

@app.route("/history/prepare")
def history_prepare_page():
    return render_template("history.html", history=enrich_prepare_history_rows(list_history(500), list_jobs(5000)))

@app.route("/history/clean")
def history_clean_page():
    return render_template("clean_logs.html", logs=list_clean_actions(500))

@app.route("/history/packing")
def history_packing_page():
    return render_template("packing_history.html", jobs=enrich_packing_history_rows(list_packing_history(5000)))

@app.route("/history/posting")
def history_posting_page():
    return render_template("posting_history.html", jobs=list_posting_history(5000))

@app.route("/api/history/export.csv")
def api_history_export():
    rows = list_history(5000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","media_type","source_path","source_rel","dest_path","source_bytes","dest_bytes","chosen_bracket","end_tag","created_at"])
    for r in rows:
        writer.writerow([r.get("id"), r.get("media_type"), r.get("source_path"), r.get("source_rel"), r.get("dest_path"), r.get("source_bytes"), r.get("dest_bytes"), r.get("chosen_bracket"), r.get("end_tag"), r.get("created_at")])
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_history.csv"})

@app.route("/api/clean/logs/export.csv")
def api_clean_logs_export():
    rows = list_clean_actions(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","reason","media_type","target_path","target_kind","dry_run","success","size_bytes","message"])
    for r in rows:
        writer.writerow([r.get("id"), r.get("created_at"), r.get("reason"), r.get("media_type"), r.get("target_path"), r.get("target_kind"), r.get("dry_run"), r.get("success"), r.get("size_bytes"), r.get("message")])
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_clean_logs.csv"})


@app.route("/api/packing/history/export.csv")
def api_packing_history_export():
    rows = list_packing_history(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","started_at","finished_at","job_name","source_path","output_root","status","phase","percent","size_bytes","rar_parts_estimate","par2_percent","archive_token","message"])
    for r in rows:
        writer.writerow([r.get("id"), r.get("created_at"), r.get("started_at"), r.get("finished_at"), r.get("job_name"), r.get("source_path"), r.get("output_root"), r.get("status"), r.get("phase"), r.get("percent"), r.get("size_bytes"), r.get("rar_parts_estimate"), r.get("par2_percent"), r.get("archive_token"), r.get("message")])
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_packing_history.csv"})

@app.route("/api/posting/history/export.csv")
def api_posting_history_export():
    rows = list_posting_history(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","started_at","finished_at","job_name","packed_root","posted_root","status","phase","percent","size_bytes","provider_used","nzb_path","message"])
    for r in rows:
        writer.writerow([r.get("id"), r.get("created_at"), r.get("started_at"), r.get("finished_at"), r.get("job_name"), r.get("packed_root"), r.get("posted_root"), r.get("status"), r.get("phase"), r.get("percent"), r.get("size_bytes"), r.get("provider_used"), r.get("nzb_path"), r.get("message")])
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_posting_history.csv"})

@app.route("/api/prepare/tv/search")
def api_prepare_tv_search():
    settings = load_settings()
    names = search_shows(settings["tv_root"], request.args.get("q", ""))
    plex_posters = {x["name"]: x.get("poster_url","") for x in search_posters_for_prepare(settings, "tv", request.args.get("q", ""))}
    results = []
    for n in names:
        local = show_poster(settings["tv_root"], n)
        results.append({"name": n, "poster_url": local or plex_posters.get(n, "")})
    return jsonify({"results": results})

@app.route("/api/prepare/tv/seasons")
def api_prepare_tv_seasons():
    return jsonify({"results": list_seasons(load_settings()["tv_root"], request.args.get("show", ""))})

@app.route("/api/prepare/tv/preview", methods=["POST"])
def api_prepare_tv_preview():
    data = request.get_json(force=True)
    return jsonify(preview_tv(load_settings(), data["show_name"], data["season_name"], data.get("bracket_override","")))

@app.route("/api/prepare/tv/start", methods=["POST"])
def api_prepare_tv_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    payload = request.get_json(force=True)
    settings = load_settings()
    job_id = create_job("tv", payload["source_path"], payload["dest_path"])
    add_job_event(job_id, "queued", "TV prepare job queued.", 0)
    threading.Thread(target=run_prepare_job_when_slot, args=(job_id, "tv", settings, payload), daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id})

@app.route("/api/prepare/movie/search")
def api_prepare_movie_search():
    settings = load_settings()
    names = search_movies(settings["movie_root"], request.args.get("q", ""))
    plex_posters = {x["name"]: x.get("poster_url","") for x in search_posters_for_prepare(settings, "movie", request.args.get("q", ""))}
    results = []
    for n in names:
        local = movie_poster(settings["movie_root"], n)
        results.append({"name": n, "poster_url": local or plex_posters.get(n, "")})
    return jsonify({"results": results})

@app.route("/api/prepare/movie/preview", methods=["POST"])
def api_prepare_movie_preview():
    data = request.get_json(force=True)
    return jsonify(preview_movie(load_settings(), data["movie_name"], data.get("bracket_override","")))

@app.route("/api/prepare/movie/start", methods=["POST"])
def api_prepare_movie_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    payload = request.get_json(force=True)
    settings = load_settings()
    job_id = create_job("movie", payload["source_path"], payload["dest_path"])
    add_job_event(job_id, "queued", "Movie prepare job queued.", 0)
    threading.Thread(target=run_prepare_job_when_slot, args=(job_id, "movie", settings, payload), daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id})

@app.route("/api/jobs")
def api_jobs():
    return jsonify(_jobs_payload())

@app.route("/api/jobs/stream")
def api_jobs_stream():
    return _event_stream(_jobs_payload)

@app.route("/api/clean/preview")
def api_clean_preview():
    settings = load_settings()
    history = list_history(500)
    filter_reason = request.args.get("reason", "both")
    filter_type = request.args.get("type", "all")
    return jsonify(preview_clean(settings, history, filter_reason, filter_type))



def _collapse_clean_candidates(candidates):
    collapsed = []
    grouped = {}
    passthrough = []

    for cand in candidates:
        target_kind = str(cand.get("target_kind", "") or "")
        media_type = str(cand.get("media_type", "") or "")
        details = cand.get("details", {}) or {}
        show_path = str(details.get("season_parent_show_path", "") or "")
        season_names = details.get("season_folder_names_in_show") or []

        if media_type == "tv" and target_kind in {"season_folder", "source_path"} and show_path and season_names:
            grouped.setdefault(show_path, []).append(cand)
        else:
            passthrough.append(cand)

    for show_path, items in grouped.items():
        all_season_names = set()
        selected_names = set()
        for item in items:
            details = item.get("details", {}) or {}
            all_season_names.update(details.get("season_folder_names_in_show") or [])
            selected_names.add(pathlib.Path(str(item.get("target_path", ""))).name)

        if all_season_names and selected_names >= all_season_names:
            primary = dict(items[0])
            details = dict(primary.get("details", {}) or {})
            details["selected_all_season_folders_in_show"] = True
            details["will_also_remove_show_folder"] = True
            details["show_folder_delete_reason"] = "All season folders for this show were selected."
            details["selected_season_folder_names"] = sorted(selected_names)
            primary["details"] = details
            primary["title"] = pathlib.Path(show_path).name
            primary["target_path"] = show_path
            primary["target_kind"] = "show_folder"
            collapsed.append(primary)
        else:
            collapsed.extend(items)

    collapsed.extend(passthrough)
    return collapsed

@app.route("/api/clean/delete", methods=["POST"])
def api_clean_delete():
    try:
        settings = load_settings()
        data = request.get_json(force=True) or {}
        confirmation = data.get("confirmation", "")
        candidates = data.get("candidates", [])
        dry_run = data.get("dry_run", settings.get("clean_dry_run","true") == "true")
        use_recycle_bin = data.get("use_recycle_bin", settings.get("clean_use_recycle_bin","true") == "true")
        recycle_bin_root = settings.get("recycle_bin_root", "/media/dest/.prepac_recycle")
        if confirmation != "DELETE":
            return jsonify({"ok": False, "error": "Confirmation must equal DELETE."}), 400
        if not candidates:
            return jsonify({"ok": False, "error": "No candidates selected."}), 400

        effective_candidates = _collapse_clean_candidates(candidates)
        results = []
        for c in effective_candidates:
            try:
                results.append(delete_candidate(c, dry_run=dry_run, use_recycle_bin=use_recycle_bin, recycle_bin_root=recycle_bin_root))
            except Exception as e:
                results.append({
                    "target_path": c.get("target_path", ""),
                    "media_type": c.get("media_type", ""),
                    "reason": c.get("reason", ""),
                    "dry_run": bool(dry_run),
                    "success": False,
                    "size_bytes": int(c.get("size_bytes", 0) or 0),
                    "breakdown": c.get("breakdown", []),
                    "message": f"Delete error: {e}",
                    "details": dict(c.get("details", {}) or {}),
                })

        successful_candidates = [c for c, r in zip(effective_candidates, results) if r.get("success")]
        if not dry_run and successful_candidates:
            try:
                plex_refresh = notify_after_clean(settings, successful_candidates)
            except Exception as e:
                plex_refresh = {"ok": False, "message": str(e), "refreshed": []}
        else:
            plex_refresh = {"ok": True, "refreshed": [], "skipped": True}
        http_status = 200 if any(r.get("success") for r in results) else 500
        return jsonify({"ok": any(r.get("success") for r in results), "results": results, "plex_refresh": plex_refresh}), http_status
    except Exception as e:
        return jsonify({"ok": False, "error": f"Clean request failed: {e}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1234, debug=False)

def is_first_run():
    s = load_settings()
    return not (s.get('auth_username') and s.get('auth_password_hash'))
