
import csv
import hashlib
import hmac
import io
import os
import logging
import pathlib
import json
import re
import threading
import atexit
import time
import secrets
import requests
from flask import Flask, jsonify, redirect, render_template, request, url_for, flash, Response, session, stream_with_context, has_request_context, send_file
from flask.sessions import SecureCookieSessionInterface
from urllib.parse import urlencode, urlparse
from jsonschema import validate, ValidationError as JsonSchemaValidationError
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.exceptions import RequestEntityTooLarge

from app.db import (
    CONFIG_DIR,
    DB_PATH,
    SCHEMA_VERSION,
    AUTH_SETTING_KEYS,
    db_is_corrupt,
    init_db,
    load_settings,
    save_settings_patch,
    update_auth_settings_atomic,
    get_conn,
)
from app.jobs import create_job, add_job_event, list_jobs, list_jobs_by_status, try_claim_prepare_slot, cancel_prepare_job, register_prepare_worker, unregister_prepare_worker, set_job_status, prepare_should_stop, acknowledge_prepare_outcome_unknown_for_resubmission, fail_prepare_job_if_active
from app.history_db import list_history, delete_prepared_by_id
from app.helpers import human_bytes
from app.clean_actions import list_clean_actions
from app.prepare_tv import search_shows, list_seasons, preview_tv
from app.prepare_movie import search_movies, preview_movie
from app.copy_engine import run_tv_prepare, run_movie_prepare
from app.plex_clean_preview import preview_clean, search_posters_for_prepare, resolve_clean_candidate_ids, clean_candidate_id, normalize_clean_filter_scope
from app.clean_engine import delete_candidate
from app.plex_auth import create_pin, check_pin, list_servers_for_token, save_selected_server, build_auth_url, choose_best_server_connection
from app.plex_notify import notify_after_clean
from app.posters import show_poster, movie_poster
from app.packing_core import scan_watch_folder, start_packing_job_async, resolve_packing_candidate
from app.packing_jobs import list_packing_jobs, list_packing_jobs_by_status, list_packing_history, get_existing_active_packing_job_id, has_outdated_or_missing_successful_packing, add_packing_event, cancel_packing_job, acknowledge_packing_outcome_unknown_for_resubmission
from app.posting_core import scan_posting_candidates, start_posting_job_async, get_posting_live_output, get_posting_live_stats, get_posting_providers, resolve_posting_candidate
from app.posting_provider_config import sanitize_posting_provider_items
from app.data_sanitizer import redact_sensitive_data
from app.posting_jobs import list_posting_jobs, list_posting_jobs_by_status, list_posting_history, get_existing_active_posting_job_id, has_outdated_or_missing_successful_posting, cancel_posting_job, acknowledge_posting_outcome_unknown_for_resubmission
from app.secret_utils import SECRET_SPECS, masked_secret_value, secret_source, resolve_secret
from app.share_core import build_resolved_category_preview, build_share_candidates, build_share_submission_review, CATEGORY_KEY_OPTIONS, get_share_destinations, import_share_bundle, import_share_bundles_bulk, list_share_history, normalize_share_base_url, public_share_destinations, queue_share_jobs, refresh_share_caps, start_share_job_async, fetch_destination_caps, remove_share_candidate
from app.share_jobs import get_share_job, list_share_jobs, increment_share_retry, force_retry_share_outcome_unknown, cancel_share_job
from app.path_guardrails import assert_no_parent_traversal, assert_operation_pair, assert_path_within_roots, build_allowed_roots
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime
from app.version import APP_NAME, APP_VERSION, BUILD_NUMBER, FULL_VERSION, DISPLAY_VERSION, BUILD_DISPLAY
from app.logging_utils import setup_logging
from app.metrics import inc, observe, render_prometheus, set_gauge
from app.config_validation import normalize_settings
from app.auth_state import auth_is_initialized
from app.fs_watcher import start_watchers, stop_watchers
from app.file_locks import release_lock, try_acquire_lock
from app.web_security import (
    build_external_base_url,
    csrf_token_matches,
    ensure_csrf_token,
    is_unsafe_http_method,
    share_import_limit_bytes,
    share_import_limit_mebibytes,
    host_is_allowed,
    is_trusted_proxy_peer,
    resolve_client_ip,
    normalize_service_base_url,
)
from app.workflow_paths import posting_posted_root, settings_with_effective_workflow_paths
from app.timestamp_utils import local_now, parse_local_timestamp




# JSON Schema for posting provider validation
_POSTING_PROVIDER_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string", "maxLength": 256},
        "name": {"type": "string", "maxLength": 256},
        "enabled": {"type": "boolean"},
        "host": {"type": "string", "maxLength": 256},
        "port": {"type": ["string", "integer"], "minimum": 1, "maximum": 65535},
        "ssl": {"type": "boolean"},
        "username": {"type": "string", "maxLength": 256},
        "password": {"type": "string", "maxLength": 128},
        "connections": {"type": ["string", "integer"], "minimum": 1, "maximum": 1000},
        "max_connections": {"type": ["string", "integer"], "minimum": 1, "maximum": 1000},
        "account_group": {"type": "string", "maxLength": 256},
        "priority_up_to_gb": {"type": ["string", "integer"], "minimum": 0, "maximum": 999999},
    },
    "additionalProperties": False,
}

def _validate_posting_providers(provider_items):
    """Validate posting provider list against schema."""
    if not isinstance(provider_items, list):
        raise JsonSchemaValidationError("Providers must be a list")
    
    for idx, item in enumerate(provider_items):
        if not isinstance(item, dict):
            raise JsonSchemaValidationError(f"Provider {idx} is not a dict")
        
        try:
            validate(instance=item, schema=_POSTING_PROVIDER_SCHEMA)
            if item.get("enabled") and not str(item.get("host") or "").strip():
                raise JsonSchemaValidationError("Enabled providers require a host")
            for key, minimum, maximum in (
                ("port", 1, 65535),
                ("connections", 1, 1000),
                ("max_connections", 1, 1000),
                ("priority_up_to_gb", 0, 999999),
            ):
                raw_value = item.get(key)
                if raw_value is None or str(raw_value).strip() == "":
                    continue
                if isinstance(raw_value, bool):
                    raise JsonSchemaValidationError(f"{key} must be an integer")
                try:
                    parsed_value = int(str(raw_value).strip(), 10)
                except (TypeError, ValueError) as exc:
                    raise JsonSchemaValidationError(f"{key} must be an integer") from exc
                if not minimum <= parsed_value <= maximum:
                    raise JsonSchemaValidationError(
                        f"{key} must be between {minimum} and {maximum}"
                    )
        except JsonSchemaValidationError as e:
            raise JsonSchemaValidationError(f"Provider {idx}: {e.message}")

def _provider_slug(value, fallback_idx):

    value = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return value or f"provider{fallback_idx}"


def _default_posting_provider(idx):
    return {
        "id": f"provider{idx}",
        "name": f"Provider {idx}",
        "enabled": False,
        "host": "",
        "port": "563",
        "ssl": True,
        "username": "",
        "password": "",
        "connections": "25",
        "max_connections": "25",
        "account_group": "",
        "priority_up_to_gb": "0" if idx == 1 else "0",
    }


def _display_posting_providers(settings, display_settings):
    source = secret_source("posting_providers_json", settings)
    providers = get_posting_providers(settings)
    normalized = []
    for idx, provider in enumerate(providers, start=1):
        item = dict(_default_posting_provider(idx))
        item.update(provider or {})
        item["id"] = _provider_slug(item.get("id") or item.get("name"), idx)
        item["name"] = str(item.get("name") or f"Provider {idx}").strip() or f"Provider {idx}"
        item["password_configured"] = bool(str(item.get("password") or "").strip())
        item["password"] = ""
        item["password_source"] = display_settings.get(f"posting_provider{idx}_password_source", "saved_setting" if str(provider.get("password") or "").strip() else "unset") if idx <= 2 else ("saved_setting" if str(provider.get("password") or "").strip() else "unset")
        item["account_group"] = str(item.get("account_group", "") or "").strip()
        item["priority_up_to_gb"] = str(item.get("priority_up_to_gb", "0") or "0").strip() or "0"
        normalized.append(item)
    while len(normalized) < 2:
        normalized.append(_default_posting_provider(len(normalized) + 1))
    editor_value = json.dumps(normalized, ensure_ascii=False, indent=2) if source == "saved_setting" else ""
    return normalized, editor_value, source


_SHARE_DESTINATION_KEYS = {
    "id", "name", "enabled", "mode", "base_url", "api_key", "basic_auth",
    "username", "password", "include_nfo", "include_mediainfo", "includemeta",
    "categories_cache", "category_overrides",
}


def _display_share_destinations(settings):
    source = secret_source("share_destinations_json", settings)
    public = []
    for idx, destination in enumerate(get_share_destinations(settings), start=1):
        entry = {key: value for key, value in destination.items() if key in _SHARE_DESTINATION_KEYS}
        entry["id"] = _provider_slug(entry.get("id") or entry.get("name"), idx)
        entry["name"] = str(entry.get("name") or f"Destination {idx}").strip() or f"Destination {idx}"
        entry["api_key_configured"] = bool(str(entry.get("api_key") or "").strip())
        entry["password_configured"] = bool(str(entry.get("password") or "").strip())
        entry["api_key"] = ""
        entry["password"] = ""
        public.append(entry)
    editor = json.dumps(public, ensure_ascii=False, indent=2) if source == "saved_setting" else ""
    return public, editor, source


def _template_safe_settings(settings):
    """Preserve non-secret UI settings without placing credentials in page HTML."""
    safe = dict(settings or {})
    secret_keys = set(SECRET_SPECS) | {
        "auth_password_hash", "auth_recovery_hash",
    }
    for key in secret_keys:
        if key in safe:
            safe[key] = ""
    safe["posting_providers_json"] = "[]"
    safe["share_destinations_json"] = "[]"
    safe["plex_token_configured"] = secret_source("plex_token", settings) != "unset"
    safe["packing_freeimage_api_key_configured"] = secret_source("packing_freeimage_api_key", settings) != "unset"
    return safe


def _sync_legacy_posting_provider_settings(data, providers):
    providers = list(providers or [])
    for idx in (1, 2):
        provider = dict(_default_posting_provider(idx))
        if len(providers) >= idx and isinstance(providers[idx - 1], dict):
            provider.update(providers[idx - 1])
        prefix = f"posting_provider{idx}_"
        data[prefix + "enabled"] = "true" if provider.get("enabled") else "false"
        data[prefix + "host"] = str(provider.get("host", "") or "").strip()
        data[prefix + "port"] = str(provider.get("port", "563") or "563").strip()
        data[prefix + "ssl"] = "true" if provider.get("ssl", True) else "false"
        data[prefix + "username"] = str(provider.get("username", "") or "").strip()
        data[prefix + "password"] = str(provider.get("password", "") or "")
        data[prefix + "connections"] = str(provider.get("connections", "25") or "25").strip()
        data[prefix + "max_connections"] = str(provider.get("max_connections", provider.get("connections", "25")) or provider.get("connections", "25") or "25").strip()

def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _metrics_token() -> str:
    return str(os.environ.get("PREPAC_METRICS_TOKEN", "") or "").strip()


def _metrics_token_valid() -> bool:
    configured = _metrics_token()
    if not configured:
        return False
    provided = str(request.headers.get("X-Prepac-Metrics-Token", "") or "").strip()
    return bool(provided) and secrets.compare_digest(provided, configured)


def _session_cookie_mode() -> str:
    mode = str(os.environ.get("PREPAC_SESSION_COOKIE_MODE", "auto") or "auto").strip().lower()
    return mode if mode in {"legacy", "auto", "always", "never"} else "legacy"


def _proxy_headers_trusted() -> bool:
    if not _bool_env("PREPAC_TRUST_PROXY_HEADERS", False):
        return False
    configured = os.environ.get("PREPAC_TRUSTED_PROXIES")
    return is_trusted_proxy_peer(request.remote_addr, configured)


def _resolve_session_cookie_secure() -> bool:
    mode = _session_cookie_mode()
    if mode == "always":
        return True
    if mode == "never":
        return False
    if mode == "legacy":
        return _bool_env("PREPAC_SESSION_COOKIE_SECURE", False)

    # auto mode: enable for direct HTTPS or trusted proxy HTTPS headers.
    if request.is_secure:
        return True
    if _proxy_headers_trusted():
        forwarded_proto = str(request.headers.get("X-Forwarded-Proto", "") or "").split(",")[0].strip().lower()
        if forwarded_proto == "https":
            return True
    return False


def _load_or_create_flask_secret() -> str:
    env_value = (os.environ.get("PREPAC_FLASK_SECRET_KEY", "") or "").strip()
    if env_value:
        return env_value
    secret_path = CONFIG_DIR / "flask_secret_key"
    secret_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    for _ in range(50):
        try:
            if secret_path.exists():
                existing = secret_path.read_text(encoding="utf-8", errors="strict").strip()
                if existing:
                    try:
                        os.chmod(secret_path, 0o600)
                    except OSError:
                        pass
                    return existing
                time.sleep(0.02)
                continue
            generated = secrets.token_urlsafe(64)
            fd = os.open(str(secret_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(generated + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return generated
        except FileExistsError:
            time.sleep(0.02)
            continue
        except OSError as exc:
            raise RuntimeError(f"Unable to create or read the persistent Flask secret at {secret_path}") from exc
    raise RuntimeError(f"Persistent Flask secret at {secret_path} remained empty or unstable")


class AdaptiveSecureCookieSessionInterface(SecureCookieSessionInterface):
    def get_cookie_secure(self, app_obj):
        if has_request_context():
            return _resolve_session_cookie_secure()
        return super().get_cookie_secure(app_obj)

setup_logging()
LOG = logging.getLogger(__name__)

app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.session_interface = AdaptiveSecureCookieSessionInterface()
app.secret_key = _load_or_create_flask_secret()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=_bool_env("PREPAC_SESSION_COOKIE_SECURE", False),
    MAX_CONTENT_LENGTH=share_import_limit_bytes(os.environ.get("PREPAC_SHARE_IMPORT_MAX_MB")),
)
init_db()

LOG.info("Session cookie mode: %s", _session_cookie_mode())




@app.context_processor
def inject_version_info():
    return {
        "app_name": APP_NAME,
        "app_version": APP_VERSION,
        "build_number": BUILD_NUMBER,
        "full_version": FULL_VERSION,
        "display_version": DISPLAY_VERSION,
        "build_display": BUILD_DISPLAY,
        "csrf_token": ensure_csrf_token(session),
    }

def auth_initialized(settings=None):
    settings = settings or load_settings()
    return auth_is_initialized(settings)

def auth_username(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_username", "") or "").strip()

def auth_password_hash(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_password_hash", "") or "").strip()

def auth_recovery_hash(settings=None):
    settings = settings or load_settings()
    return str(settings.get("auth_recovery_hash", "") or "").strip()


_COMMON_PASSWORDS = {
    "admin", "changeme", "letmein", "password", "password1", "prepac",
    "qwerty", "welcome", "12345678", "123456789", "1234567890",
}
_AUTH_DUMMY_HASH = generate_password_hash(secrets.token_urlsafe(32))


def _password_policy_error(password: str) -> str | None:
    candidate = str(password or "")
    if len(candidate) < 12:
        return "Password must be at least 12 characters."
    if candidate.casefold() in _COMMON_PASSWORDS:
        return "Choose a less common password."
    return None


def _password_requires_change(password: str) -> bool:
    return _password_policy_error(password) is not None


def _next_auth_session_epoch(settings) -> int:
    return _auth_session_epoch(settings) + 1


def _establish_authenticated_session(settings, username: str):
    session.clear()
    ensure_csrf_token(session)
    session["auth_ok"] = True
    session["auth_user"] = str(username or "")
    session["auth_epoch"] = _auth_session_epoch(settings)

def reset_token_configured():
    token = resolve_secret("auth_password_reset_token", {})
    return bool(str(token or "").strip())

def _auth_session_epoch(settings=None):
    settings = settings or load_settings()
    try:
        return max(1, int(str(settings.get("auth_session_epoch", "1") or "1")))
    except Exception:
        return 1


def is_authenticated(settings=None):
    if session.get("auth_ok") is not True:
        return False
    settings = settings or load_settings()
    try:
        return int(session.get("auth_epoch", 0) or 0) == _auth_session_epoch(settings)
    except Exception:
        return False


def current_external_base_url():
    return build_external_base_url(
        request.scheme,
        request.host,
        forwarded_proto=request.headers.get("X-Forwarded-Proto", ""),
        forwarded_host=request.headers.get("X-Forwarded-Host", ""),
        trust_proxy=_bool_env("PREPAC_TRUST_PROXY_HEADERS", False),
        peer_address=request.remote_addr,
        trusted_proxy_networks=os.environ.get("PREPAC_TRUSTED_PROXIES"),
        trusted_hosts=os.environ.get("PREPAC_TRUSTED_HOSTS"),
    )


def _safe_next_url(candidate: str | None):
    target = (candidate or "").strip()
    if not target:
        return url_for("dashboard")
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return url_for("dashboard")
    if not target.startswith("/"):
        return url_for("dashboard")
    if target.startswith("//"):
        return url_for("dashboard")
    return target


def _current_request_relative_url():
    query = request.query_string.decode("utf-8", errors="ignore")
    return f"{request.path}?{query}" if query else request.path


def _share_import_limit_mib():
    return share_import_limit_mebibytes(os.environ.get("PREPAC_SHARE_IMPORT_MAX_MB"))


@app.before_request
def _track_request_start():
    request._prepac_started_at = time.time()


@app.before_request
def _validate_request_host():
    try:
        if not host_is_allowed(request.host, os.environ.get("PREPAC_TRUSTED_HOSTS")):
            raise ValueError("host is not allowed")
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid or untrusted Host header"}), 400


@app.before_request
def _ensure_csrf_session_token():
    ensure_csrf_token(session)


@app.before_request
def _enforce_csrf():
    if not is_unsafe_http_method(request.method):
        return None

    provided = (
        request.headers.get("X-CSRF-Token")
        or request.headers.get("X-XSRF-Token")
        or request.form.get("csrf_token")
    )
    if csrf_token_matches(session, provided):
        return None

    message = "Security check failed. Reload the page and try again."
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({"ok": False, "error": message}), 400

    flash(message, "error")
    return redirect(_safe_next_url(_current_request_relative_url()))


@app.before_request
def _limit_json_request_bodies():
    content_length = request.content_length
    if request.path.startswith("/api/") and request.is_json:
        if content_length is not None and content_length > 1024 * 1024:
            return jsonify({"ok": False, "error": "JSON request body is limited to 1 MiB"}), 413
        if is_unsafe_http_method(request.method):
            payload = request.get_json(silent=True)
            if not isinstance(payload, dict):
                return jsonify({"ok": False, "error": "A valid JSON object is required"}), 400
    if (
        is_unsafe_http_method(request.method)
        and request.endpoint not in {"api_share_import", "api_share_import_bulk"}
        and content_length is not None
        and content_length > 2 * 1024 * 1024
    ):
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Request body is limited to 2 MiB"}), 413
        return "Request body too large", 413

@app.after_request
def _track_request_metrics(response):
    started_at = getattr(request, "_prepac_started_at", None)
    if started_at:
        observe("prepac_http_request_seconds", max(0.0, time.time() - started_at), method=request.method, endpoint=request.endpoint or "unknown")
    inc("prepac_http_requests", 1, method=request.method, status=str(response.status_code))
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; "
        "connect-src 'self'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'",
    )
    if request.endpoint != "static":
        response.headers.setdefault("Cache-Control", "no-store")
    if _bool_env("PREPAC_ENABLE_HSTS", False) and _resolve_session_cookie_secure():
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000")
    return response


@app.errorhandler(RequestEntityTooLarge)
def _handle_request_entity_too_large(_exc):
    limit_mib = _share_import_limit_mib()
    if request.endpoint in {"api_share_import", "api_share_import_bulk"}:
        return jsonify({
            "ok": False,
            "error": f"Upload too large. Share imports are limited to {limit_mib} MiB per request. Adjust PREPAC_SHARE_IMPORT_MAX_MB if needed.",
            "limit_mib": limit_mib,
        }), 413
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Request body too large"}), 413
    flash("Request body too large", "error")
    return redirect(_safe_next_url(_current_request_relative_url()))


@app.errorhandler(404)
def _handle_not_found(_exc):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Not found"}), 404
    return render_template("404.html"), 404


@app.errorhandler(500)
def _handle_internal_error(_exc):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Internal server error"}), 500
    return render_template("500.html"), 500


def _database_unavailable_response(exc):
    safe_message = redact_sensitive_data(str(exc))[:300]
    LOG.error(
        "Settings database unavailable for %s (%s): %s",
        request.path,
        type(exc).__name__,
        safe_message,
    )
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({
            "ok": False,
            "error": "Configuration database temporarily unavailable",
        }), 503
    return render_template(
        "500.html",
        error_status=503,
        error_label="Service unavailable",
        error_eyebrow="Database unavailable",
        error_title="PrepaC cannot read its configuration database",
        error_message=(
            "Wait a moment and try again. If the issue continues, check the "
            "application logs and configuration storage before restarting active work."
        ),
    ), 503


@app.before_request
def enforce_authentication():
    endpoint = request.endpoint or ""
    allowed = {"health_page", "login_page", "setup_page", "reset_password_page", "static", "metrics_page"}
    # Liveness and static assets must not depend on a readable settings database.
    if endpoint in {"health_page", "static"}:
        return None
    try:
        settings = load_settings()
    except Exception as exc:
        return _database_unavailable_response(exc)

    if endpoint in allowed:
        if endpoint == "metrics_page":
            # Backward-compatible default: still require auth when no metrics token is configured.
            # If a token is configured, allow non-interactive scraping with that token.
            if not _metrics_token():
                if not auth_initialized(settings):
                    return jsonify({"ok": False, "error": "Authentication setup required"}), 403
                if not is_authenticated(settings):
                    return jsonify({"ok": False, "error": "Authentication required"}), 401
                return None
            if _metrics_token_valid() or is_authenticated(settings):
                return None
            return jsonify({"ok": False, "error": "Metrics token required"}), 401
        if endpoint == "setup_page":
            return None
        if not auth_initialized(settings) and endpoint != "setup_page":
            return redirect(url_for("setup_page"))
        return None

    if not auth_initialized(settings):
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Authentication setup required"}), 403
        return redirect(url_for("setup_page"))

    if not is_authenticated(settings):
        if session.get("auth_ok"):
            session.clear()
            ensure_csrf_token(session)
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Authentication required"}), 401
        return redirect(url_for("login_page", next=request.path))

    if str(settings.get("auth_force_password_change", "false")).lower() == "true" and endpoint not in {"change_password_page", "logout_page", "static"}:
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Password change required", "code": "password_change_required"}), 403
        return redirect(url_for("change_password_page", forced="1"))

APP_RUNTIME_STATE = {
    "draining": False,
}
AUTH_RATE_STATE = {}
AUTH_RATE_LOCK = threading.Lock()


def _client_ip_address() -> str:
    if _proxy_headers_trusted():
        return resolve_client_ip(
            request.remote_addr,
            request.headers.get("X-Forwarded-For", ""),
            os.environ.get("PREPAC_TRUSTED_PROXIES"),
        )[:64]
    return str(request.remote_addr or "unknown")[:64]


def _auth_rate_limits() -> tuple[int, int, int]:
    try:
        window_seconds = max(10, int(str(os.environ.get("PREPAC_AUTH_RATE_WINDOW_SECONDS", "300") or "300")))
    except Exception:
        window_seconds = 300
    try:
        max_attempts = max(3, int(str(os.environ.get("PREPAC_AUTH_RATE_MAX_ATTEMPTS", "20") or "20")))
    except Exception:
        max_attempts = 20
    try:
        lockout_seconds = max(10, int(str(os.environ.get("PREPAC_AUTH_LOCKOUT_SECONDS", "600") or "600")))
    except Exception:
        lockout_seconds = 600
    return window_seconds, max_attempts, lockout_seconds


def _auth_rate_keys(kind: str, username: str) -> tuple[str, str]:
    supplied = str(username or "").strip().casefold()
    expected = auth_username().casefold()
    account_bucket = "known" if supplied and secrets.compare_digest(supplied, expected) else "unknown"
    return (
        f"{kind}:ip:{_client_ip_address()}",
        f"{kind}:account:{account_bucket}",
    )


def _auth_rate_read_state(conn, key, now_ts, window_seconds):
    row = conn.execute(
        "SELECT failures_json, locked_until FROM auth_rate_limits WHERE rate_key=?",
        (key,),
    ).fetchone()
    if not row:
        return [], 0.0
    try:
        values = json.loads(row["failures_json"] or "[]")
    except Exception:
        values = []
    failures = [float(ts) for ts in values if now_ts - float(ts) <= window_seconds]
    return failures, float(row["locked_until"] or 0.0)


def _auth_rate_write_state(conn, key, failures, locked_until, now_ts):
    conn.execute(
        """INSERT INTO auth_rate_limits(rate_key, failures_json, locked_until, updated_at)
           VALUES(?, ?, ?, ?)
           ON CONFLICT(rate_key) DO UPDATE SET
             failures_json=excluded.failures_json,
             locked_until=excluded.locked_until,
             updated_at=excluded.updated_at""",
        (key, json.dumps(failures[-100:]), float(locked_until or 0.0), now_ts),
    )


def _auth_rate_check(kind: str, username: str) -> tuple[bool, int]:
    now_ts = time.time()
    window_seconds, max_attempts, lockout_seconds = _auth_rate_limits()
    rate_keys = _auth_rate_keys(kind, username)
    with AUTH_RATE_LOCK:
        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            retry_after = 0
            for key in rate_keys:
                failures, locked_until = _auth_rate_read_state(conn, key, now_ts, window_seconds)
                if locked_until <= now_ts and len(failures) >= max_attempts:
                    locked_until = now_ts + lockout_seconds
                _auth_rate_write_state(conn, key, failures, locked_until if locked_until > now_ts else 0.0, now_ts)
                retry_after = max(retry_after, int(max(0, round(locked_until - now_ts))))
            conn.commit()
            return (retry_after <= 0), max(1, retry_after) if retry_after else 0
        finally:
            conn.close()


def _auth_rate_record_failure(kind: str, username: str):
    now_ts = time.time()
    window_seconds, max_attempts, lockout_seconds = _auth_rate_limits()
    rate_keys = _auth_rate_keys(kind, username)
    with AUTH_RATE_LOCK:
        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for key in rate_keys:
                failures, locked_until = _auth_rate_read_state(conn, key, now_ts, window_seconds)
                failures.append(now_ts)
                if len(failures) >= max_attempts:
                    locked_until = max(locked_until, now_ts + lockout_seconds)
                _auth_rate_write_state(conn, key, failures, locked_until, now_ts)
            stale_before = now_ts - max(window_seconds, lockout_seconds) * 2
            conn.execute("DELETE FROM auth_rate_limits WHERE updated_at < ?", (stale_before,))
            count = int(conn.execute("SELECT COUNT(*) FROM auth_rate_limits").fetchone()[0])
            if count > 2048:
                conn.execute(
                    "DELETE FROM auth_rate_limits WHERE rate_key IN (SELECT rate_key FROM auth_rate_limits ORDER BY updated_at ASC LIMIT ?)",
                    (count - 2048,),
                )
            conn.commit()
        finally:
            conn.close()


def _auth_rate_clear(kind: str, username: str):
    rate_keys = _auth_rate_keys(kind, username)
    with AUTH_RATE_LOCK:
        conn = get_conn()
        try:
            conn.executemany("DELETE FROM auth_rate_limits WHERE rate_key=?", [(key,) for key in rate_keys])
            conn.commit()
        finally:
            conn.close()

# Reconcile process-local queues before watchers can enqueue new work. Only the
# lock owner performs this one-time recovery and runs the background reconciler.
_RECONCILIATION_LOCK_FILE = str(CONFIG_DIR / "prepac_reconciliation.lock")
_RECONCILIATION_LOCK_HANDLE = None
try:
    _RECONCILIATION_LOCK_HANDLE = try_acquire_lock(_RECONCILIATION_LOCK_FILE)
    if _RECONCILIATION_LOCK_HANDLE is None:
        raise OSError("Reconciliation lock already held")
    from app.job_reconciliation import background_reconciliation_loop, reconcile_abandoned_queued_jobs
    recovered_queued_jobs = reconcile_abandoned_queued_jobs()
    if recovered_queued_jobs:
        LOG.warning("Recovered %s abandoned queued jobs during startup", recovered_queued_jobs)
    threading.Thread(target=background_reconciliation_loop, daemon=True).start()
except OSError:
    # Another worker already runs reconciliation; skip in this worker.
    release_lock(_RECONCILIATION_LOCK_HANDLE)
    _RECONCILIATION_LOCK_HANDLE = None
except Exception as exc:
    release_lock(_RECONCILIATION_LOCK_HANDLE)
    _RECONCILIATION_LOCK_HANDLE = None
    LOG.warning("Unable to start background job reconciliation: %s", exc)

_WATCHER_LOCK_FILE = str(CONFIG_DIR / "prepac_watcher.lock")
_WATCHER_LOCK_HANDLE = None
try:
    _WATCHER_LOCK_HANDLE = try_acquire_lock(_WATCHER_LOCK_FILE)
    if _WATCHER_LOCK_HANDLE is None:
        raise OSError("Watcher lock already held")
    start_watchers(load_settings())
except OSError:
    # Another worker already holds the watcher lock; skip in this worker.
    release_lock(_WATCHER_LOCK_HANDLE)
    _WATCHER_LOCK_HANDLE = None
except Exception as exc:
    release_lock(_WATCHER_LOCK_HANDLE)
    _WATCHER_LOCK_HANDLE = None
    LOG.warning("Unable to start file-system watchers: %s", exc)


def begin_graceful_shutdown(reason="Application shutdown requested"):
    """Stop accepting work and wait briefly for this process's workers only."""
    if APP_RUNTIME_STATE.get("draining"):
        return
    APP_RUNTIME_STATE["draining"] = True
    LOG.info("%s; draining local workers", reason)

    try:
        stop_watchers()
    except Exception as exc:
        LOG.warning("Unable to stop file-system watchers cleanly: %s", exc)

    try:
        configured_timeout = float(
            str(os.environ.get("PREPAC_SHUTDOWN_DRAIN_SECONDS", "10") or "10").strip()
        )
    except (TypeError, ValueError):
        configured_timeout = 10.0
    drain_timeout = max(0.0, min(120.0, configured_timeout))

    try:
        from app.job_reconciliation import wait_for_local_workers
        drained, active = wait_for_local_workers(timeout_seconds=drain_timeout)
    except Exception as exc:
        LOG.warning("Unable to inspect local workers during shutdown: %s", exc)
        return
    if not drained:
        active_counts = {kind: len(job_ids) for kind, job_ids in active.items() if job_ids}
        LOG.warning(
            "Shutdown drain timed out after %.1fs with local workers still active: %s",
            drain_timeout,
            active_counts,
        )


atexit.register(lambda: begin_graceful_shutdown(reason="Application exiting"))


@app.context_processor
def _inject_db_status():
    from app.db import db_is_corrupt
    return {"db_corrupt": db_is_corrupt()}


@app.route("/api/db/integrity")
def api_db_integrity():
    if not is_authenticated():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    from app.db import run_db_integrity_check
    return jsonify(run_db_integrity_check())


@app.route("/api/db/repair", methods=["POST"])
def api_db_repair():
    """Rebuild all DB indexes in-place (REINDEX). Safe; no data is modified."""
    if not is_authenticated():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    from app.db import run_db_reindex
    result = run_db_reindex()
    status_code = 200 if result.get("ok") else 500
    return jsonify(result), status_code


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
        value = max(0, int(num_bytes or 0))
    except Exception:
        value = 0
    return human_bytes(value)

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

def _parse_iso(ts):
    return parse_local_timestamp(ts)

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
    }

    if APP_RUNTIME_STATE.get("draining"):
        payload["status"] = "draining"
        payload["reason"] = "container is draining for graceful shutdown"
        return True, payload

    # A missing database must remain missing; get_conn() otherwise creates it and
    # could make an uninitialized installation appear healthy.
    try:
        if not DB_PATH.is_file() or DB_PATH.stat().st_size <= 0 or db_is_corrupt():
            raise RuntimeError("database is unavailable")
        conn = get_conn()
        try:
            if conn.execute("SELECT 1 FROM settings LIMIT 1").fetchone() is None:
                raise RuntimeError("settings table is empty")
            version_row = conn.execute("PRAGMA user_version").fetchone()
            if not version_row or int(version_row[0]) < SCHEMA_VERSION:
                raise RuntimeError("database schema is not current")
        finally:
            conn.close()
    except Exception as e:
        payload["status"] = "error"
        payload["db"] = "error"
        payload["reason"] = f"db probe failed: {e}"
        return False, payload

    now_dt = local_now()
    thresholds = {
        "prepare": 10 * 60,
        "packing": 15 * 60,
        "posting": 10 * 60,
    }

    try:
        prepare_jobs = list_jobs_by_status(["running", "finalizing"], 500)
        packing_jobs = list_packing_jobs_by_status(["running", "finalizing"], 500)
        posting_jobs = list_posting_jobs_by_status(["running", "finalizing"], 500)
    except Exception as e:
        payload["status"] = "error"
        payload["reason"] = f"job listing failed: {e}"
        return False, payload

    def inspect(kind, jobs, threshold_seconds):
        stalled = []
        running = [
            j for j in jobs
            if str(j.get("status", "")).lower() in {"running", "finalizing"}
        ]
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
        payload["status"] = "degraded"
        first = payload["stalled"][0]
        payload["reason"] = f"{first['kind']} job stalled for {first['seconds_since_activity']} seconds"
        return True, payload

    return True, payload

def _prepare_queue_unavailable(exc, media_type):
    safe_message = redact_sensitive_data(str(exc))[:300]
    LOG.error(
        "Unable to persist %s Prepare queue submission (%s): %s",
        media_type,
        type(exc).__name__,
        safe_message,
        exc_info=True,
    )
    return jsonify({
        "ok": False,
        "error": "Prepare queue is temporarily unavailable. Try again in a few seconds.",
    }), 503


_PREPARE_BATCH_LIMIT = 50
_PREPARE_PREVIEW_TOKEN_MAX_AGE_SECONDS = 3600
_PREPARE_PREVIEW_TOKEN_SALT = "prepac-prepare-preview-v1"


def _prepare_child_name(value, label, max_length):
    value = str(value or "").strip()
    if (
        not value
        or len(value) > int(max_length)
        or value in {".", ".."}
        or pathlib.Path(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"A valid {label} is required.")
    return value


def _prepare_preview_token(preview):
    media_type = str(preview.get("media_type") or "").strip().lower()
    data = {
        "version": 1,
        "media_type": media_type,
        "source_path": str(preview.get("source_path") or ""),
        "dest_path": str(preview.get("dest_path") or ""),
        "queue_bracket": str(preview.get("queue_bracket") or ""),
        "bracket_is_resolved": preview.get("bracket_is_resolved") is True,
    }
    if media_type == "tv":
        data.update({
            "show_name": str(preview.get("show_name") or ""),
            "season_name": str(preview.get("season_name") or ""),
        })
    elif media_type == "movie":
        data["movie_name"] = str(preview.get("movie_name") or "")
    else:
        raise ValueError("Prepare preview has an unsupported media type.")
    if not data["source_path"] or not data["dest_path"] or not data["bracket_is_resolved"]:
        raise ValueError("Prepare preview is incomplete.")
    return URLSafeTimedSerializer(
        app.secret_key,
        salt=_PREPARE_PREVIEW_TOKEN_SALT,
    ).dumps(data)


def _load_prepare_preview_token(value):
    token = str(value or "").strip()
    if not token or len(token) > 16384:
        raise ValueError("Build a new Prepare preview before starting.")
    try:
        data = URLSafeTimedSerializer(
            app.secret_key,
            salt=_PREPARE_PREVIEW_TOKEN_SALT,
        ).loads(token, max_age=_PREPARE_PREVIEW_TOKEN_MAX_AGE_SECONDS)
    except SignatureExpired as exc:
        raise ValueError("The Prepare preview expired. Build a new preview.") from exc
    except BadSignature as exc:
        raise ValueError("The Prepare preview is invalid. Build a new preview.") from exc
    if not isinstance(data, dict) or data.get("version") != 1:
        raise ValueError("The Prepare preview is invalid. Build a new preview.")
    return data


def _create_prepare_queue_entry(settings, media_type, payload):
    """Persist one lightweight selection without repeating media probes."""
    if not isinstance(payload, dict):
        raise ValueError("Each Prepare item must be an object.")

    preview = _load_prepare_preview_token(payload.get("preview_token"))
    requested_media_type = str(media_type or "").strip().lower()
    media_type = str(preview.get("media_type") or "").strip().lower()
    if requested_media_type and requested_media_type != media_type:
        raise ValueError("Prepare item kind does not match its preview.")
    queue_bracket = str(preview.get("queue_bracket") or "").strip()
    if len(queue_bracket) > 256 or preview.get("bracket_is_resolved") is not True:
        raise ValueError("The Prepare preview is invalid. Build a new preview.")
    source_value = str(preview.get("source_path") or "")
    destination_value = str(preview.get("dest_path") or "")
    if (
        not source_value
        or not destination_value
        or len(source_value) > 8192
        or len(destination_value) > 8192
    ):
        raise ValueError("The Prepare preview paths are invalid. Build a new preview.")

    if media_type == "tv":
        show_name = _prepare_child_name(preview.get("show_name"), "show name", 512)
        season_name = _prepare_child_name(preview.get("season_name"), "season name", 256)
        source_operation = "prepare_tv_source"
        source_label = "TV season"
        initial_message = "TV prepare job queued."
        worker_payload = {
            "show_name": show_name,
            "season_name": season_name,
            "queue_bracket": queue_bracket,
            "bracket_is_resolved": True,
        }
    elif media_type == "movie":
        movie_name = _prepare_child_name(preview.get("movie_name"), "movie name", 512)
        source_operation = "prepare_movie_source"
        source_label = "movie"
        initial_message = "Movie prepare job queued."
        worker_payload = {
            "movie_name": movie_name,
            "queue_bracket": queue_bracket,
            "bracket_is_resolved": True,
        }
    else:
        raise ValueError("Prepare item kind must be tv or movie.")

    try:
        source_path, destination_path = assert_operation_pair(
            source_value,
            destination_value,
            settings,
            source_operation,
            "prepare_destination",
            source_label=source_label,
            destination_label="prepare destination",
        )
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    if not source_path.is_dir():
        raise ValueError(f"The selected {source_label} is not a directory.")
    worker_payload.update({
        "expected_source_path": str(source_path),
        "expected_dest_path": str(destination_path),
    })

    idempotency_key = (
        f"prepare:{media_type}:"
        + hashlib.sha256(
            f"{source_path}\0{destination_path}".encode("utf-8")
        ).hexdigest()
    )
    job_id, created = create_job(
        media_type,
        str(source_path),
        str(destination_path),
        idempotency_key=idempotency_key,
        return_created=True,
        initial_event=("queued", initial_message, 0),
    )
    result = {
        "ok": True,
        "job_id": job_id,
        "duplicate": not created,
    }
    launch = None
    if created:
        launch = (job_id, media_type, settings, worker_payload)
    return result, launch


def _launch_prepare_worker(job_id, media_type, settings, worker_payload):
    try:
        threading.Thread(
            target=run_prepare_job_when_slot,
            args=(job_id, media_type, settings, worker_payload),
            daemon=True,
        ).start()
        return True
    except Exception as exc:
        safe_message = redact_sensitive_data(str(exc))[:300]
        LOG.error(
            "Unable to launch %s Prepare worker for job %s (%s): %s",
            media_type,
            job_id,
            type(exc).__name__,
            safe_message,
            exc_info=True,
        )
        try:
            fail_prepare_job_if_active(
                job_id,
                "Prepare worker could not be started; submit this item again.",
                "dispatch_failed",
            )
        except Exception as cleanup_exc:
            LOG.error(
                "Unable to terminalize undispatched Prepare job %s (%s): %s",
                job_id,
                type(cleanup_exc).__name__,
                redact_sensitive_data(str(cleanup_exc))[:300],
                exc_info=True,
            )
        return False


_OUTCOME_UNKNOWN_CONFIRMATION = "I VERIFIED THE DESTINATION"


def _acknowledge_ambiguous_outcome(workflow, acknowledge):
    """Release deduplication only after an explicit administrator attestation."""
    payload = request.get_json(silent=True) or {}
    try:
        job_id = int(payload.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if job_id <= 0:
        return jsonify({"ok": False, "error": "A valid job_id is required"}), 400
    if payload.get("acknowledge_ambiguous_outcome") is not True:
        return jsonify({
            "ok": False,
            "error": "Explicit ambiguous-outcome acknowledgement is required",
        }), 400
    confirmation = str(payload.get("confirmation") or "")
    if len(confirmation) > 128 or not hmac.compare_digest(
        confirmation,
        _OUTCOME_UNKNOWN_CONFIRMATION,
    ):
        return jsonify({
            "ok": False,
            "error": f'Type "{_OUTCOME_UNKNOWN_CONFIRMATION}" exactly after verifying the destination',
        }), 400
    if not acknowledge(job_id):
        return jsonify({
            "ok": False,
            "error": f"Only outcome_unknown {workflow.lower()} jobs can be acknowledged",
        }), 409
    return jsonify({
        "ok": True,
        "job_id": job_id,
        "status": "failed",
        "resubmission_unblocked": True,
        "warning": (
            f"{workflow} job #{job_id} is now eligible for a fresh manual submission. "
            "The prior operation may still have completed, so resubmission can duplicate its effects."
        ),
    })

def run_prepare_job_when_slot(job_id, media_type, settings, payload):
    register_prepare_worker(job_id)
    last_wait_event_ts = 0.0
    last_wait_max_jobs = None
    wait_started_ts = time.monotonic()
    try:
        wait_cfg = load_settings().get("prepare_slot_wait_timeout_seconds", "")
        if str(wait_cfg or "").strip() == "":
            wait_cfg = os.environ.get("PREPAC_PREPARE_SLOT_WAIT_TIMEOUT_SECONDS", "0")
        configured_wait = int(str(wait_cfg or "0").strip())
        max_wait_seconds = 0 if configured_wait <= 0 else max(300, configured_wait)
    except Exception:
        max_wait_seconds = 0
    try:
        if prepare_should_stop(job_id):
            return
        while True:
            if prepare_should_stop(job_id):
                return
            current = load_settings()
            try:
                max_jobs = max(1, int(current.get("prepare_max_concurrent_jobs", settings.get("prepare_max_concurrent_jobs", "1")) or 1))
            except Exception:
                max_jobs = 1
            if prepare_should_stop(job_id):
                return
            waited = int(time.monotonic() - wait_started_ts)
            if max_wait_seconds and waited >= max_wait_seconds:
                add_job_event(job_id, "failed", f"Timed out waiting for prepare slot after {waited} seconds.", None)
                set_job_status(job_id, "failed")
                return
            if try_claim_prepare_slot(job_id, max_jobs):
                break
            now_ts = time.monotonic()
            if (now_ts - last_wait_event_ts) >= 15.0 or last_wait_max_jobs != max_jobs:
                add_job_event(job_id, "queued", f"Waiting for prepare slot ({max_jobs} max concurrent jobs).", 0)
                last_wait_event_ts = now_ts
                last_wait_max_jobs = max_jobs
            time.sleep(1)
        if prepare_should_stop(job_id):
            return
        if media_type == "tv":
            run_tv_prepare(job_id, load_settings(), payload)
        else:
            run_movie_prepare(job_id, load_settings(), payload)
    except Exception as exc:
        safe_message = redact_sensitive_data(str(exc))[:300]
        LOG.error(
            "Prepare worker %s stopped before completion (%s): %s",
            job_id,
            type(exc).__name__,
            safe_message,
            exc_info=True,
        )
        try:
            fail_prepare_job_if_active(
                job_id,
                "Prepare worker stopped before the copy completed. Submit the item again.",
                "worker_failed",
            )
        except Exception as cleanup_exc:
            LOG.error(
                "Unable to terminalize failed Prepare worker %s (%s): %s",
                job_id,
                type(cleanup_exc).__name__,
                redact_sensitive_data(str(cleanup_exc))[:300],
                exc_info=True,
            )
    finally:
        unregister_prepare_worker(job_id)

def _valid_job_duration_seconds(started_at, finished_at):
    if not started_at or not finished_at:
        return None
    try:
        started = parse_local_timestamp(started_at)
        finished = parse_local_timestamp(finished_at)
        if not started or not finished:
            return None
        seconds = int((finished - started).total_seconds())
        return seconds if seconds >= 0 else None
    except Exception:
        return None


def _job_duration_seconds(started_at, finished_at):
    duration = _valid_job_duration_seconds(started_at, finished_at)
    if duration is None:
        return 0
    return duration

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
        if "recycle" in msg:
            summary["recycle_actions"] += 1
        if is_dry:
            summary["dry_runs"] += 1
            summary["bytes_dry_run"] += size
        else:
            summary["real_runs"] += 1
            if is_success:
                summary["successes"] += 1
                summary["bytes_total"] += size
                summary["bytes_real"] += size
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




def _packing_output_bytes(job):
    return (
        int(job.get("rar_size_bytes", 0) or 0)
        + int(job.get("par2_size_bytes", 0) or 0)
    )


def summarize_packing_stats(packing_jobs):
    completed = [j for j in packing_jobs if str(j.get("status","")).lower() == "done"]
    total_bytes = sum(_packing_output_bytes(j) for j in completed)
    durations = []
    largest = 0
    for j in completed:
        largest = max(largest, _packing_output_bytes(j))
        duration = _valid_job_duration_seconds(
            j.get("started_at"),
            j.get("finished_at"),
        )
        if duration is not None:
            durations.append(duration)
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
    for j in completed:
        largest = max(largest, int(j.get("size_bytes", 0) or 0))
        duration = _valid_job_duration_seconds(
            j.get("started_at"),
            j.get("finished_at"),
        )
        if duration is not None:
            durations.append(duration)
    avg_seconds = int(sum(durations)/len(durations)) if durations else 0
    return {
        "total_jobs": len(completed),
        "total_bytes": total_bytes,
        "largest_bytes": largest,
        "avg_seconds": avg_seconds,
    }



def parse_posting_log_stats(log_path):
    import re

    p = pathlib.Path(log_path)
    stats = {"transfer_rate": "", "percent_transferred": "", "eta": ""}
    if not p.exists():
        return stats
    try:
        size = p.stat().st_size
        with p.open("rb") as handle:
            head = handle.read(64 * 1024)
            if size > 1024 * 1024:
                handle.seek(max(0, size - 1024 * 1024))
            tail = handle.read(1024 * 1024)
        text = (head + b"\n" + tail).decode("utf-8", errors="replace")
    except OSError:
        return stats
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


def summarize_share_stats(share_jobs):
    rows = list(share_jobs or [])
    total_jobs = len(rows)
    done_jobs = sum(1 for row in rows if str(row.get("status", "")).lower() == "done")
    failed_jobs = sum(1 for row in rows if str(row.get("status", "")).lower() == "failed")
    queued_jobs = sum(1 for row in rows if str(row.get("status", "")).lower() == "queued")
    running_jobs = sum(1 for row in rows if str(row.get("status", "")).lower() in {"running", "uploading"})
    attention_jobs = sum(1 for row in rows if str(row.get("status", "")).lower() == "outcome_unknown")
    destinations = sorted({str(row.get("destination_name") or row.get("destination_id") or "").strip() for row in rows if str(row.get("destination_name") or row.get("destination_id") or "").strip()})
    imported_jobs = sum(1 for row in rows if str(row.get("source_type", "")).lower() == "imported")
    posting_jobs = sum(1 for row in rows if str(row.get("source_type", "")).lower() == "posting")
    return {
        "total_jobs": total_jobs,
        "done_jobs": done_jobs,
        "failed_jobs": failed_jobs,
        "queued_jobs": queued_jobs,
        "running_jobs": running_jobs,
        "attention_jobs": attention_jobs,
        "destination_count": len(destinations),
        "imported_jobs": imported_jobs,
        "posting_jobs": posting_jobs,
    }


_DASHBOARD_STAT_KEYS = (
    "prepare.completed_jobs",
    "prepare.prepared_items",
    "prepare.tv_items",
    "prepare.movie_items",
    "prepare.source_bytes",
    "prepare.prepared_bytes",
    "packing.completed_jobs",
    "packing.output_bytes",
    "packing.largest_output",
    "packing.avg_duration",
    "posting.completed_jobs",
    "posting.posted_bytes",
    "posting.largest_job",
    "posting.avg_duration",
    "share.all_jobs",
    "share.successful",
    "share.failed",
    "share.destinations_used",
    "clean.actions",
    "clean.actual_actions",
    "clean.successful",
    "clean.removed_bytes",
)
_DASHBOARD_STATS_CACHE_TTL_SECONDS = 10.0
_DASHBOARD_STATS_CACHE_LOCK = threading.Lock()
_DASHBOARD_STATS_CACHE = {"expires_at": 0.0, "payload": None}


def _dashboard_average_duration(conn, table_name):
    if table_name not in {"packing_jobs", "posting_jobs"}:
        raise ValueError("Unsupported dashboard duration source")
    rows = conn.execute(
        f"SELECT started_at, finished_at FROM {table_name} WHERE status='done'"
    ).fetchall()
    durations = []
    for row in rows:
        duration = _valid_job_duration_seconds(row["started_at"], row["finished_at"])
        if duration is not None:
            durations.append(duration)
    return int(sum(durations) / len(durations)) if durations else 0


def _dashboard_all_time_stats():
    """Read every dashboard total from one consistent SQLite snapshot."""
    conn = get_conn()
    try:
        conn.execute("BEGIN")
        prepare_items = conn.execute(
            """
            SELECT
                COUNT(*) AS total_prepared_items,
                COALESCE(SUM(CASE WHEN LOWER(media_type)='tv' THEN 1 ELSE 0 END), 0) AS tv_items,
                COALESCE(SUM(CASE WHEN LOWER(media_type)='movie' THEN 1 ELSE 0 END), 0) AS movie_items,
                COALESCE(SUM(source_bytes), 0) AS source_bytes_total,
                COALESCE(SUM(dest_bytes), 0) AS dest_bytes_total
            FROM prepared_items
            """
        ).fetchone()
        prepare_jobs = conn.execute(
            "SELECT COUNT(*) AS total_prepare_jobs FROM prepare_jobs WHERE status='done'"
        ).fetchone()
        packing = conn.execute(
            """
            WITH completed AS (
                SELECT
                    COALESCE(rar_size_bytes, 0) + COALESCE(par2_size_bytes, 0) AS output_bytes
                FROM packing_jobs
                WHERE status='done'
            )
            SELECT
                COUNT(*) AS total_jobs,
                COALESCE(SUM(output_bytes), 0) AS total_bytes,
                COALESCE(MAX(output_bytes), 0) AS largest_bytes,
                COALESCE(SUM(CASE WHEN output_bytes <= 0 THEN 1 ELSE 0 END), 0) AS unmeasured_jobs
            FROM completed
            """
        ).fetchone()
        posting = conn.execute(
            """
            SELECT
                COUNT(*) AS total_jobs,
                COALESCE(SUM(size_bytes), 0) AS total_bytes,
                COALESCE(MAX(size_bytes), 0) AS largest_bytes
            FROM posting_jobs
            WHERE status='done'
            """
        ).fetchone()
        share = conn.execute(
            """
            SELECT
                COUNT(*) AS total_jobs,
                COALESCE(SUM(CASE WHEN status='done' THEN 1 ELSE 0 END), 0) AS done_jobs,
                COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), 0) AS failed_jobs,
                COALESCE(SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END), 0) AS queued_jobs,
                COALESCE(SUM(CASE WHEN status IN ('running','uploading') THEN 1 ELSE 0 END), 0) AS running_jobs,
                COALESCE(SUM(CASE WHEN status='outcome_unknown' THEN 1 ELSE 0 END), 0) AS attention_jobs,
                COALESCE(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END), 0) AS cancelled_jobs,
                COUNT(DISTINCT COALESCE(
                    NULLIF(TRIM(destination_id), ''),
                    NULLIF(TRIM(destination_name), '')
                )) AS destination_count,
                COALESCE(SUM(CASE WHEN source_type='imported' THEN 1 ELSE 0 END), 0) AS imported_jobs,
                COALESCE(SUM(CASE WHEN source_type='posting' THEN 1 ELSE 0 END), 0) AS posting_jobs
            FROM share_jobs
            """
        ).fetchone()
        clean = conn.execute(
            """
            SELECT
                COUNT(*) AS total_actions,
                COALESCE(SUM(CASE WHEN LOWER(dry_run)='true' THEN 1 ELSE 0 END), 0) AS dry_runs,
                COALESCE(SUM(CASE WHEN LOWER(dry_run)!='true' THEN 1 ELSE 0 END), 0) AS real_runs,
                COALESCE(SUM(CASE
                    WHEN LOWER(dry_run)!='true' AND LOWER(success)='true' THEN 1 ELSE 0
                END), 0) AS successes,
                COALESCE(SUM(CASE
                    WHEN LOWER(dry_run)!='true' AND LOWER(success)!='true' THEN 1 ELSE 0
                END), 0) AS failures,
                COALESCE(SUM(CASE
                    WHEN LOWER(dry_run)='true' THEN 0
                    WHEN json_valid(details_json) THEN
                        CASE
                            WHEN json_type(details_json, '$.logical_bytes_removed') IN ('integer', 'real')
                            THEN MAX(0, CAST(json_extract(details_json, '$.logical_bytes_removed') AS INTEGER))
                            WHEN LOWER(success)='true' THEN COALESCE(size_bytes, 0)
                            ELSE 0
                        END
                    WHEN LOWER(success)='true' THEN COALESCE(size_bytes, 0)
                    ELSE 0
                END), 0) AS bytes_total,
                COALESCE(SUM(CASE
                    WHEN LOWER(dry_run)='true' THEN size_bytes ELSE 0
                END), 0) AS bytes_dry_run
            FROM clean_actions
            """
        ).fetchone()
        return {
            "prepare": {
                "total_prepare_jobs": int(prepare_jobs["total_prepare_jobs"] or 0),
                "total_prepared_items": int(prepare_items["total_prepared_items"] or 0),
                "tv_items": int(prepare_items["tv_items"] or 0),
                "movie_items": int(prepare_items["movie_items"] or 0),
                "source_bytes_total": int(prepare_items["source_bytes_total"] or 0),
                "dest_bytes_total": int(prepare_items["dest_bytes_total"] or 0),
            },
            "packing": {
                "total_jobs": int(packing["total_jobs"] or 0),
                "total_bytes": int(packing["total_bytes"] or 0),
                "largest_bytes": int(packing["largest_bytes"] or 0),
                "unmeasured_jobs": int(packing["unmeasured_jobs"] or 0),
                "avg_seconds": _dashboard_average_duration(conn, "packing_jobs"),
            },
            "posting": {
                "total_jobs": int(posting["total_jobs"] or 0),
                "total_bytes": int(posting["total_bytes"] or 0),
                "largest_bytes": int(posting["largest_bytes"] or 0),
                "avg_seconds": _dashboard_average_duration(conn, "posting_jobs"),
            },
            "share": {key: int(share[key] or 0) for key in (
                "total_jobs", "done_jobs", "failed_jobs", "queued_jobs",
                "running_jobs", "attention_jobs", "cancelled_jobs",
                "destination_count", "imported_jobs", "posting_jobs",
            )},
            "clean": {
                "total_actions": int(clean["total_actions"] or 0),
                "dry_runs": int(clean["dry_runs"] or 0),
                "real_runs": int(clean["real_runs"] or 0),
                "successes": int(clean["successes"] or 0),
                "failures": int(clean["failures"] or 0),
                "bytes_total": int(clean["bytes_total"] or 0),
                "bytes_dry_run": int(clean["bytes_dry_run"] or 0),
                "bytes_real": int(clean["bytes_total"] or 0),
            },
        }
    finally:
        conn.close()


def _dashboard_stats_display(stats):
    def number(value):
        return f"{int(value or 0):,}"

    return {
        "prepare.completed_jobs": number(stats["prepare"]["total_prepare_jobs"]),
        "prepare.prepared_items": number(stats["prepare"]["total_prepared_items"]),
        "prepare.tv_items": number(stats["prepare"]["tv_items"]),
        "prepare.movie_items": number(stats["prepare"]["movie_items"]),
        "prepare.source_bytes": humansize_filter(stats["prepare"]["source_bytes_total"]),
        "prepare.prepared_bytes": humansize_filter(stats["prepare"]["dest_bytes_total"]),
        "packing.completed_jobs": number(stats["packing"]["total_jobs"]),
        "packing.output_bytes": humansize_filter(stats["packing"]["total_bytes"]),
        "packing.largest_output": humansize_filter(stats["packing"]["largest_bytes"]),
        "packing.avg_duration": humanduration_filter(stats["packing"]["avg_seconds"]),
        "posting.completed_jobs": number(stats["posting"]["total_jobs"]),
        "posting.posted_bytes": humansize_filter(stats["posting"]["total_bytes"]),
        "posting.largest_job": humansize_filter(stats["posting"]["largest_bytes"]),
        "posting.avg_duration": humanduration_filter(stats["posting"]["avg_seconds"]),
        "share.all_jobs": number(stats["share"]["total_jobs"]),
        "share.successful": number(stats["share"]["done_jobs"]),
        "share.failed": number(stats["share"]["failed_jobs"]),
        "share.destinations_used": number(stats["share"]["destination_count"]),
        "clean.actions": number(stats["clean"]["total_actions"]),
        "clean.actual_actions": number(stats["clean"]["real_runs"]),
        "clean.successful": number(stats["clean"]["successes"]),
        "clean.removed_bytes": humansize_filter(stats["clean"]["bytes_total"]),
    }


def _dashboard_stats_payload():
    now = time.monotonic()
    with _DASHBOARD_STATS_CACHE_LOCK:
        cached = _DASHBOARD_STATS_CACHE["payload"]
        if cached is not None and now < _DASHBOARD_STATS_CACHE["expires_at"]:
            return cached
        stats = _dashboard_all_time_stats()
        payload = {
            "ok": True,
            "stats": stats,
            "display": _dashboard_stats_display(stats),
        }
        _DASHBOARD_STATS_CACHE["payload"] = payload
        _DASHBOARD_STATS_CACHE["expires_at"] = time.monotonic() + _DASHBOARD_STATS_CACHE_TTL_SECONDS
        return payload


def _dashboard_unavailable_display():
    return {key: "Unavailable" for key in _DASHBOARD_STAT_KEYS}


def summarize_running_jobs(prepare_jobs, packing_jobs, posting_jobs):
    running = []
    for j in prepare_jobs:
        status = str(j.get("status", "")).lower()
        if status in ("queued", "running", "finalizing", "outcome_unknown"):
            running.append({"kind":"Prepare","title":j.get("source_path",""),"status":status,"phase":j.get("phase",""),"percent":j.get("percent"),"message":j.get("message","")})
    for j in packing_jobs:
        status = str(j.get("status", "")).lower()
        if status in ("queued", "running", "finalizing", "outcome_unknown"):
            running.append({"kind":"Packing","title":j.get("job_name",""),"status":status,"phase":j.get("phase",""),"percent":j.get("percent"),"message":j.get("message","")})
    for j in posting_jobs:
        status = str(j.get("status", "")).lower()
        if status in ("queued", "running", "finalizing", "outcome_unknown"):
            running.append({"kind":"Posting","title":j.get("job_name",""),"status":status,"phase":j.get("phase",""),"percent":j.get("percent"),"message":j.get("message","")})
    return running

def _tail_text_file(path_str, max_lines=120):
    p = pathlib.Path(path_str)
    if not p.exists():
        return ""
    try:
        with p.open("rb") as handle:
            size = handle.seek(0, os.SEEK_END)
            handle.seek(max(0, size - 512 * 1024))
            chunk = handle.read(512 * 1024)
        lines = chunk.decode("utf-8", errors="replace").splitlines(keepends=True)
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
        try:
            max_stream_seconds = max(10, int(str(os.environ.get("PREPAC_SSE_MAX_SECONDS", "55") or "55")))
        except Exception:
            max_stream_seconds = 55
        try:
            heartbeat_seconds = max(5, int(str(os.environ.get("PREPAC_SSE_HEARTBEAT_SECONDS", "15") or "15")))
        except Exception:
            heartbeat_seconds = 15
        started = time.monotonic()
        last_emit = 0.0
        yield "retry: 5000\n\n"
        while (time.monotonic() - started) < max_stream_seconds:
            try:
                payload = generator_fn()
            except Exception as exc:
                LOG.warning("SSE payload generation failed: %s", redact_sensitive_data(str(exc)))
                payload = {"ok": False, "error": "Unable to refresh live data"}
            data = _sse_json(payload)
            if data != last:
                yield data
                last = data
                last_emit = time.monotonic()
            elif (time.monotonic() - last_emit) >= heartbeat_seconds:
                yield ": keepalive\n\n"
                last_emit = time.monotonic()
            time.sleep(1)
    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

def _dashboard_running_payload():
    all_jobs = list_jobs_by_status(["queued", "running", "finalizing", "outcome_unknown"], 500)
    all_packing_jobs = list_packing_jobs_by_status(["queued", "running", "finalizing", "outcome_unknown"], 500)
    all_posting_jobs = list_posting_jobs_by_status(["queued", "running", "finalizing", "outcome_unknown"], 500)
    return {"ok": True, "running": summarize_running_jobs(all_jobs, all_packing_jobs, all_posting_jobs)}

def _prepare_active_jobs_payload():
    try:
        active_jobs = list_jobs_by_status(["queued", "running", "finalizing", "outcome_unknown"], 500)
        active_jobs.sort(key=lambda j: int(j.get("id") or 0))
    except Exception as exc:
        LOG.warning("Prepare job listing failed: %s", redact_sensitive_data(str(exc)))
        return {"jobs": [], "ok": False, "error": "Prepare jobs could not be listed"}
    return {
        "jobs": active_jobs,
        "ok": True,
    }

def _prepare_history_jobs(limit=500):
    jobs = [j for j in list_jobs(limit) if str(j.get("status", "")).lower() in {"done", "failed", "cancelled", "outcome_unknown"}]
    for j in jobs:
        j["duration_seconds"] = _job_duration_seconds(j.get("started_at"), j.get("finished_at"))
    return jobs


_PUBLIC_JOB_SECRET_KEYS = {
    "password", "password_value", "header_value", "provider_lock",
    "raw_response", "nzb_hash", "job_hash", "idempotency_key",
}


def _public_api_value(value):
    """Return a detached, recursively redacted value suitable for JSON/SSE clients."""
    if isinstance(value, dict):
        cleaned = {
            str(key): _public_api_value(item)
            for key, item in value.items()
            if str(key).casefold() not in _PUBLIC_JOB_SECRET_KEYS
        }
        return cleaned
    if isinstance(value, list):
        return [_public_api_value(item) for item in value]
    if isinstance(value, tuple):
        return [_public_api_value(item) for item in value]
    return redact_sensitive_data(value) if isinstance(value, str) else value

def _packing_jobs_payload():
    jobs = list_packing_jobs(200)
    for j in jobs:
        if j.get("events"):
            latest = j["events"][0]
            j["message"] = latest.get("message", j.get("message",""))
            j["phase"] = latest.get("phase", j.get("phase",""))
            j["percent"] = latest.get("percent", j.get("percent"))
    active_statuses = {"queued", "running", "finalizing", "outcome_unknown"}
    jobs.sort(key=lambda j: (0 if str(j.get("status", "")).lower() in active_statuses else 1, int(j.get("id") or 0) if str(j.get("status", "")).lower() in active_statuses else -int(j.get("id") or 0)))
    return {"ok": True, "jobs": [_public_api_value(job) for job in jobs]}


_SHARE_LIVE_JOB_FIELDS = {
    "id", "job_name", "destination_id", "destination_name",
    "selected_category_id", "selected_category_label", "status", "phase",
    "percent", "message", "remote_id", "remote_guid", "created_at",
    "started_at", "finished_at", "retry_count", "events",
}


def _share_jobs_payload():
    """Return the small operational view used by the Share page and SSE."""
    try:
        rows = list_share_jobs(200, per_job_event_limit=10)
        active_statuses = {"queued", "running", "uploading", "outcome_unknown"}
        active = [
            row for row in rows
            if str(row.get("status", "")).lower() in active_statuses
        ]
        recent_failed = [
            row for row in rows
            if str(row.get("status", "")).lower() == "failed"
        ][:25]
        selected = active + recent_failed
        jobs = [
            _public_api_value({
                key: value
                for key, value in row.items()
                if key in _SHARE_LIVE_JOB_FIELDS
            })
            for row in selected
        ]
        return {"ok": True, "jobs": jobs}
    except Exception as exc:
        LOG.warning("Share job listing failed: %s", redact_sensitive_data(str(exc)))
        return {"ok": False, "jobs": [], "error": "Share jobs could not be listed"}


def _packing_completed_payload():
    jobs = enrich_packing_history_rows(list_packing_history(50))
    jobs = [j for j in jobs if str(j.get("status","")).lower() == "done"][:10]
    return {"ok": True, "jobs": [_public_api_value(job) for job in jobs]}

def _posting_jobs_payload():
    jobs = list_posting_jobs(200)
    settings = load_settings()
    for j in jobs:
        live = get_posting_live_stats(int(j.get("id", 0)))
        if any(live.values()):
            j["runtime_stats"] = live
        else:
            posted_root = j.get("posted_root") or str(posting_posted_root(settings) / str(j.get("job_name", "")))
            log_path = pathlib.Path(posted_root) / "posting.log"
            j["runtime_stats"] = parse_posting_log_stats(str(log_path))
        if j.get("events"):
            latest = j["events"][0]
            j["message"] = latest.get("message", j.get("message",""))
            j["phase"] = latest.get("phase", j.get("phase",""))
            j["percent"] = latest.get("percent", j.get("percent"))
    active_statuses = {"queued", "running", "finalizing", "outcome_unknown"}
    jobs.sort(key=lambda j: (0 if str(j.get("status", "")).lower() in active_statuses else 1, int(j.get("id") or 0) if str(j.get("status", "")).lower() in active_statuses else -int(j.get("id") or 0)))
    return {"ok": True, "jobs": [_public_api_value(job) for job in jobs]}



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



UPDATE_CACHE_TTL_SECONDS = 6 * 60 * 60

def _semver_tuple(version_text):
    raw = str(version_text or "").strip().lower()
    if raw.startswith("v"):
        raw = raw[1:]
    parts = raw.split(".")
    nums = []
    for part in parts[:3]:
        digits = "".join(ch for ch in part if ch.isdigit())
        nums.append(int(digits or 0))
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums)

def _github_release_config(settings=None):
    settings = settings or load_settings()
    owner = (settings.get("github_repo_owner") or "HoodStar1").strip()
    repo = (settings.get("github_repo_name") or "PrepaC").strip()
    slug_pattern = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
    if not slug_pattern.fullmatch(owner) or not slug_pattern.fullmatch(repo):
        raise ValueError("GitHub owner and repository must be simple names")
    return owner, repo

def _update_cache_file():
    p = CONFIG_DIR / "update_check_cache.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _load_update_cache():
    p = _update_cache_file()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_update_cache(payload):
    cache_path = _update_cache_file()
    temp_path = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        with temp_path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(temp_path, 0o600)
        except OSError:
            pass
        os.replace(temp_path, cache_path)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass

def check_latest_release(force=False):
    settings = load_settings()
    owner, repo = _github_release_config(settings)
    cache = _load_update_cache()
    now_ts = int(time.time())
    if not force and cache.get("checked_at_epoch") and now_ts - int(cache.get("checked_at_epoch", 0)) < UPDATE_CACHE_TTL_SECONDS:
        return cache

    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "PrepaC-UpdateChecker"}
    current_version = APP_VERSION
    payload = {
        "ok": False,
        "current_version": current_version,
        "current_display": DISPLAY_VERSION,
        "latest_version": current_version,
        "latest_tag": f"v{current_version}",
        "update_available": False,
        "release_url": "",
        "asset_name": "",
        "asset_url": "",
        "checked_at_epoch": now_ts,
        "checked_at_display": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }
    try:
        r = requests.get(api_url, headers=headers, timeout=(5, 12), allow_redirects=False)
        r.raise_for_status()
        content = r.content
        if len(content) > 2 * 1024 * 1024:
            raise ValueError("GitHub release response exceeded 2 MiB")
        data = json.loads(content.decode("utf-8"))
        latest_tag = str(data.get("tag_name") or f"v{current_version}")
        latest_version = latest_tag[1:] if latest_tag.startswith("v") else latest_tag
        assets = data.get("assets") or []
        preferred_asset = next((a for a in assets if str(a.get("name", "")).lower().endswith(".zip") and "source code" not in str(a.get("name", "")).lower()), None)
        payload.update({
            "ok": True,
            "latest_version": latest_version,
            "latest_tag": latest_tag,
            "update_available": _semver_tuple(latest_version) > _semver_tuple(current_version),
            "release_url": data.get("html_url") or "",
            "asset_name": (preferred_asset or {}).get("name", ""),
            "asset_url": (preferred_asset or {}).get("browser_download_url", "") or (data.get("html_url") or ""),
        })
    except Exception as e:
        payload["error"] = str(e)

    _save_update_cache(payload)
    return payload



@app.route("/api/version/check", methods=["POST"])
def api_version_check():
    settings = load_settings()
    if str(settings.get("update_check_enabled", "true")).lower() != "true":
        return jsonify({
            "ok": True,
            "current_version": APP_VERSION,
            "current_display": DISPLAY_VERSION,
            "latest_version": APP_VERSION,
            "latest_tag": f"v{APP_VERSION}",
            "update_available": False,
            "disabled": True,
            "release_url": "",
            "asset_name": "",
            "asset_url": "",
        })
    return jsonify(check_latest_release(force=True))

@app.route("/health")
def health_page():
    if APP_RUNTIME_STATE.get("draining"):
        return jsonify({"status": "draining"}), 503
    try:
        if not DB_PATH.is_file() or DB_PATH.stat().st_size <= 0 or db_is_corrupt():
            raise RuntimeError("database is unavailable")
        conn = get_conn()
        try:
            if conn.execute("SELECT 1 FROM settings LIMIT 1").fetchone() is None:
                raise RuntimeError("settings table is empty")
            version_row = conn.execute("PRAGMA user_version").fetchone()
            if not version_row or int(version_row[0]) < SCHEMA_VERSION:
                raise RuntimeError("database schema is not current")
        finally:
            conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as exc:
        LOG.error(
            "Health database probe failed (%s): %s",
            type(exc).__name__,
            redact_sensitive_data(str(exc))[:300],
        )
        return jsonify({"status": "error"}), 503

def workflow_auto_chain_enabled(settings=None):
    settings = settings or load_settings()
    return str(settings.get("workflow_auto_chain_enabled", "false")).lower() == "true"

@app.route("/metrics")
def metrics_page():
    healthy, payload = _evaluate_health_state()
    set_gauge("prepac_running_jobs", payload.get("running", {}).get("prepare", 0), kind="prepare")
    set_gauge("prepac_running_jobs", payload.get("running", {}).get("packing", 0), kind="packing")
    set_gauge("prepac_running_jobs", payload.get("running", {}).get("posting", 0), kind="posting")
    return Response(render_prometheus(), mimetype="text/plain; version=0.0.4")


@app.route("/api/debug/job-status")
def api_debug_job_status():
    """Debug endpoint showing current job queue state and stuck jobs."""
    if not _bool_env("PREPAC_ENABLE_DEBUG_ENDPOINTS", False):
        return jsonify({"ok": False, "error": "Not found"}), 404
    try:
        from app.job_reconciliation import STALE_THRESHOLDS
        from datetime import datetime
        
        def get_job_age_seconds(job):
            """Get seconds since the latest persisted activity for this job."""
            last_activity = _latest_job_activity(job)
            if last_activity:
                return int((local_now() - last_activity).total_seconds())
            return None
        
        prepare_jobs = list_jobs_by_status(["running", "finalizing"], 100)
        packing_jobs = list_packing_jobs_by_status(["running", "finalizing"], 100)
        posting_jobs = list_posting_jobs_by_status(["running", "finalizing"], 100)
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "prepare": {
                "running_count": len(prepare_jobs),
                "stale_threshold_seconds": STALE_THRESHOLDS.get("prepare", 1800),
                "jobs": []
            },
            "packing": {
                "running_count": len(packing_jobs),
                "stale_threshold_seconds": STALE_THRESHOLDS.get("packing", 2700),
                "jobs": []
            },
            "posting": {
                "running_count": len(posting_jobs),
                "stale_threshold_seconds": STALE_THRESHOLDS.get("posting", 1200),
                "jobs": []
            }
        }
        
        for job in prepare_jobs:
            age = get_job_age_seconds(job)
            result["prepare"]["jobs"].append({
                "id": job.get("id"),
                "source_path": job.get("source_path"),
                "status": job.get("status"),
                "age_seconds": age,
                "phase": job.get("phase", ""),
                "message": job.get("message", ""),
                "is_stale": age and age > STALE_THRESHOLDS.get("prepare", 1800)
            })
        
        for job in packing_jobs:
            age = get_job_age_seconds(job)
            result["packing"]["jobs"].append({
                "id": job.get("id"),
                "source_path": job.get("source_path"),
                "status": job.get("status"),
                "age_seconds": age,
                "phase": job.get("phase", ""),
                "message": job.get("message", ""),
                "is_stale": age and age > STALE_THRESHOLDS.get("packing", 2700)
            })
        
        for job in posting_jobs:
            age = get_job_age_seconds(job)
            result["posting"]["jobs"].append({
                "id": job.get("id"),
                "job_name": job.get("job_name"),
                "status": job.get("status"),
                "provider_used": job.get("provider_used", ""),
                "age_seconds": age,
                "phase": job.get("phase", ""),
                "message": job.get("message", ""),
                "is_stale": age and age > STALE_THRESHOLDS.get("posting", 1200)
            })
        
        return jsonify(_public_api_value(result)), 200
    except Exception as exc:
        LOG.warning("Debug job status failed: %s", redact_sensitive_data(str(exc)))
        return jsonify({"ok": False, "error": "Unable to build job status"}), 500


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


AUTO_CHAIN_LOCK_FILE = str(CONFIG_DIR / "prepac_auto_chain.lock")
AUTO_CHAIN_LOCK_HANDLE = None

def start_auto_chain_thread_once():
    global AUTO_CHAIN_LOCK_HANDLE
    if AUTO_CHAIN_LOCK_HANDLE is not None:
        return
    AUTO_CHAIN_LOCK_HANDLE = try_acquire_lock(AUTO_CHAIN_LOCK_FILE)
    if AUTO_CHAIN_LOCK_HANDLE is None:
        AUTO_CHAIN_LOCK_HANDLE = None
        return
    threading.Thread(target=auto_chain_loop, daemon=True).start()

def reject_if_draining():
    if APP_RUNTIME_STATE.get("draining"):
        return jsonify({"ok": False, "error": "PrepaC is draining for graceful shutdown. New jobs are temporarily blocked."}), 503
    return None


def _bounded_string_list(payload, key, *, max_items=200, max_length=4096):
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")
    values = payload.get(key)
    if not isinstance(values, list) or not values:
        raise ValueError(f"{key} must be a non-empty list")
    if len(values) > max_items:
        raise ValueError(f"{key} is limited to {max_items} items")
    result = []
    for value in values:
        item = str(value or "").strip()
        if not item or len(item) > max_length:
            raise ValueError(f"Each {key} value must contain 1 to {max_length} characters")
        result.append(item)
    return result


def _bounded_share_submission(payload):
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")
    items = payload.get("items")
    if not isinstance(items, list) or not items or len(items) > 200 or any(not isinstance(item, dict) for item in items):
        raise ValueError("items must contain between 1 and 200 objects")
    if any(len(json.dumps(item, ensure_ascii=False)) > 32 * 1024 for item in items):
        raise ValueError("Each share item is limited to 32 KiB")
    destination_ids = _bounded_string_list(payload, "destination_ids", max_items=50, max_length=256)
    if len(items) * len(destination_ids) > 200:
        raise ValueError("A share request is limited to 200 item/destination combinations")
    return items, destination_ids



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

        if not username or len(username) > 128:
            flash("Username must contain between 1 and 128 characters.", "error")
        elif any(len(value) > 1024 for value in (password, confirm, recovery, recovery_confirm)):
            flash("Credential fields are too long.", "error")
        elif password != confirm:
            flash("Passwords do not match.", "error")
        elif _password_policy_error(password):
            flash(_password_policy_error(password), "error")
        elif recovery != recovery_confirm:
            flash("Recovery secrets do not match.", "error")
        elif len(recovery) < 12:
            flash("Recovery secret must be at least 12 characters.", "error")
        else:
            updated = update_auth_settings_atomic(
                {
                    "auth_username": username,
                    "auth_password_hash": generate_password_hash(password),
                    "auth_recovery_hash": generate_password_hash(recovery),
                    "auth_initialized": "true",
                    "auth_force_password_change": "false",
                },
                expected={"auth_initialized": str(settings.get("auth_initialized", "false"))},
            )
            if updated is None:
                flash("Account setup was already completed in another request. Please sign in.", "warning")
                return redirect(url_for("login_page"))
            flash("Admin account created. Please sign in.", "success")
            return redirect(url_for("login_page"))

    return render_template("setup.html", step=1)

start_auto_chain_thread_once()

@app.route("/login", methods=["GET", "POST"])
def login_page():
    settings = load_settings()
    if not auth_initialized(settings):
        return redirect(url_for("setup_page"))
    next_url = _safe_next_url(request.values.get("next"))
    if request.method == "POST":
        username = (request.form.get("username", "") or "").strip()
        password = request.form.get("password", "") or ""
        if len(username) > 128 or len(password) > 1024:
            _auth_rate_record_failure("login", username[:128])
            flash("Invalid username or password.", "error")
            return render_template("login.html", next_url=next_url), 400
        allowed, retry_after = _auth_rate_check("login", username)
        if not allowed:
            flash(f"Too many sign-in attempts. Try again in about {retry_after} seconds.", "error")
            return render_template("login.html", next_url=next_url), 429, {"Retry-After": str(retry_after)}
        if username == auth_username(settings) and check_password_hash(auth_password_hash(settings), password):
            force_change = _password_requires_change(password)
            if force_change and str(settings.get("auth_force_password_change", "false")).lower() != "true":
                updated = update_auth_settings_atomic(
                    {"auth_force_password_change": "true"},
                    expected={
                        "auth_username": auth_username(settings),
                        "auth_password_hash": auth_password_hash(settings),
                    },
                    increment_session_epoch=False,
                )
                if updated is None:
                    flash("Credentials changed during sign-in. Please try again.", "warning")
                    return redirect(url_for("login_page"))
                settings = {**settings, **updated}
            _establish_authenticated_session(settings, username)
            _auth_rate_clear("login", username)
            if force_change or str(settings.get("auth_force_password_change", "false")).lower() == "true":
                flash("Please replace the legacy password before continuing.", "warning")
                return redirect(url_for("change_password_page", forced="1"))
            flash("Logged in successfully.", "success")
            return redirect(_safe_next_url(next_url))
        _auth_rate_record_failure("login", username)
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

        if len(username) > 128 or any(len(value) > 1024 for value in (recovery_secret, new_password, confirm)):
            _auth_rate_record_failure("reset_password", username[:128])
            flash("Unable to reset the password with the provided credentials.", "error")
            return render_template("reset_password.html"), 400

        allowed, retry_after = _auth_rate_check("reset_password", username)
        if not allowed:
            flash(f"Too many reset attempts. Try again in about {retry_after} seconds.", "error")
            return render_template("reset_password.html"), 429, {"Retry-After": str(retry_after)}

        # Validate the new credential before checking recovery credentials so
        # field-level feedback cannot be used as a recovery-secret oracle.
        if new_password != confirm:
            flash("Passwords do not match.", "error")
        elif _password_policy_error(new_password):
            flash(_password_policy_error(new_password), "error")
        else:
            username_ok = secrets.compare_digest(username.casefold(), auth_username(settings).casefold())
            configured_recovery_hash = auth_recovery_hash(settings)
            recovery_ok = check_password_hash(configured_recovery_hash or _AUTH_DUMMY_HASH, recovery_secret)
            if not username_ok or not configured_recovery_hash or not recovery_ok:
                _auth_rate_record_failure("reset_password", username)
                flash("Unable to reset the password with the provided credentials.", "error")
                return render_template("reset_password.html")
            data = update_auth_settings_atomic(
                {
                    "auth_password_hash": generate_password_hash(new_password),
                    "auth_force_password_change": "false",
                },
                expected={
                    "auth_username": auth_username(settings),
                    "auth_password_hash": auth_password_hash(settings),
                    "auth_recovery_hash": configured_recovery_hash,
                },
            )
            if data is None:
                flash("Credentials changed during the reset. Please try again.", "warning")
                return redirect(url_for("reset_password_page"))
            _auth_rate_clear("reset_password", username)
            session.clear()
            ensure_csrf_token(session)
            flash("Password reset successful. Please sign in.", "success")
            return redirect(url_for("login_page"))
    return render_template("reset_password.html")

@app.route("/logout", methods=["POST"])
def logout_page():
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("login_page"))


@app.route("/change-password", methods=["GET", "POST"])
def change_password_page():
    settings = load_settings()
    if request.method == "POST":
        current_password = request.form.get("current_password", "") or ""
        new_password = request.form.get("new_password", "") or ""
        confirm = request.form.get("confirm_password", "") or ""
        if any(len(value) > 1024 for value in (current_password, new_password, confirm)):
            flash("Credential fields are too long.", "error")
        elif not check_password_hash(auth_password_hash(settings), current_password):
            flash("Current password is incorrect.", "error")
        elif new_password != confirm:
            flash("Passwords do not match.", "error")
        elif check_password_hash(auth_password_hash(settings), new_password):
            flash("The new password must be different from the current password.", "error")
        elif _password_policy_error(new_password):
            flash(_password_policy_error(new_password), "error")
        else:
            data = update_auth_settings_atomic(
                {
                    "auth_password_hash": generate_password_hash(new_password),
                    "auth_force_password_change": "false",
                },
                expected={
                    "auth_username": auth_username(settings),
                    "auth_password_hash": auth_password_hash(settings),
                },
            )
            if data is None:
                flash("Credentials changed before the password update completed. Please try again.", "warning")
                return redirect(url_for("login_page"))
            _establish_authenticated_session(data, auth_username(data))
            flash("Password updated. Other signed-in sessions were revoked.", "success")
            return redirect(url_for("dashboard"))
    return render_template(
        "change_password.html",
        forced=str(settings.get("auth_force_password_change", "false")).lower() == "true",
    )


@app.route("/change-recovery-secret", methods=["GET", "POST"])
def change_recovery_secret_page():
    settings = load_settings()
    if not auth_initialized(settings):
        return redirect(url_for("setup_page"))
    if not is_authenticated(settings):
        return redirect(url_for("login_page", next=url_for("change_recovery_secret_page")))
    if request.method == "POST":
        current_password = request.form.get("current_password", "") or ""
        new_secret = request.form.get("new_recovery_secret", "") or ""
        confirm_secret = request.form.get("confirm_recovery_secret", "") or ""

        if any(len(value) > 1024 for value in (current_password, new_secret, confirm_secret)):
            flash("Credential fields are too long.", "error")
        elif not check_password_hash(auth_password_hash(settings), current_password):
            flash("Current password is incorrect.", "error")
        elif new_secret != confirm_secret:
            flash("Recovery secrets do not match.", "error")
        elif len(new_secret) < 12:
            flash("Recovery secret must be at least 12 characters.", "error")
        else:
            data = update_auth_settings_atomic(
                {"auth_recovery_hash": generate_password_hash(new_secret)},
                expected={
                    "auth_username": auth_username(settings),
                    "auth_password_hash": auth_password_hash(settings),
                    "auth_recovery_hash": auth_recovery_hash(settings),
                },
            )
            if data is None:
                flash("Credentials changed before the recovery update completed. Please try again.", "warning")
                return redirect(url_for("change_recovery_secret_page"))
            _establish_authenticated_session(data, auth_username(data))
            flash("Recovery secret updated. Other signed-in sessions were revoked.", "success")
            return redirect(url_for("settings_page"))
    return render_template("change_recovery_secret.html", has_recovery=bool(auth_recovery_hash(settings)))

@app.route("/")
def dashboard():
    settings = load_settings()
    try:
        dashboard_stats = _dashboard_stats_payload()
    except Exception as exc:
        LOG.error(
            "Dashboard statistics read failed (%s): %s",
            type(exc).__name__,
            redact_sensitive_data(str(exc))[:300],
            exc_info=True,
        )
        dashboard_stats = {
            "ok": False,
            "stats": {
                "prepare": {},
                "packing": {},
                "posting": {},
                "share": {},
                "clean": {},
            },
            "display": _dashboard_unavailable_display(),
        }
    try:
        all_history = list_history(10)
    except Exception as exc:
        LOG.error("Dashboard prepare history read failed: %s", exc)
        all_history = []
    try:
        all_clean_logs = list_clean_actions(10)
    except Exception as exc:
        LOG.error("Dashboard clean log read failed: %s", exc)
        all_clean_logs = []
    try:
        all_packing_jobs = list_packing_jobs(10)
    except Exception as exc:
        LOG.error("Dashboard packing jobs read failed: %s", exc)
        all_packing_jobs = []
    try:
        all_posting_jobs = list_posting_jobs(10)
    except Exception as exc:
        LOG.error("Dashboard posting jobs read failed: %s", exc)
        all_posting_jobs = []
    try:
        current_running = _dashboard_running_payload()["running"]
    except Exception as exc:
        LOG.error("Dashboard active jobs read failed: %s", exc)
        current_running = []
    recent_actions = build_recent_actions(all_history, all_clean_logs, all_packing_jobs, all_posting_jobs, 10)
    summaries = dashboard_stats["stats"]
    return render_template(
        "dashboard.html",
        settings=_template_safe_settings(settings),
        clean_summary=summaries["clean"],
        prepare_summary=summaries["prepare"],
        packing_summary=summaries["packing"],
        posting_summary=summaries["posting"],
        share_summary=summaries["share"],
        dashboard_display=dashboard_stats["display"],
        dashboard_stats_ok=dashboard_stats.get("ok") is True,
        current_running=current_running,
        recent_actions=recent_actions,
    )


@app.route("/api/dashboard/running")
def api_dashboard_running():
    return jsonify(_dashboard_running_payload())

@app.route("/api/dashboard/running/stream")
def api_dashboard_running_stream():
    return _event_stream(_dashboard_running_payload)


@app.route("/api/dashboard/stats")
def api_dashboard_stats():
    try:
        return jsonify(_dashboard_stats_payload())
    except Exception as exc:
        LOG.warning(
            "Dashboard statistics refresh failed (%s): %s",
            type(exc).__name__,
            redact_sensitive_data(str(exc))[:300],
        )
        return jsonify({
            "ok": False,
            "error": "Dashboard statistics are temporarily unavailable.",
        }), 503


HELP_TOPICS = [
    {"slug": "getting-started", "title": "Getting Started"},
    {"slug": "prepare", "title": "Prepare"},
    {"slug": "packing", "title": "Packing"},
    {"slug": "posting", "title": "Posting"},
    {"slug": "share", "title": "Share"},
    {"slug": "clean", "title": "Clean"},
    {"slug": "settings", "title": "Settings"},
]

@app.route("/help")
def help_page():
    topic = request.args.get("topic", "getting-started")
    valid = {t["slug"] for t in HELP_TOPICS}
    if topic not in valid:
        topic = "getting-started"
    return render_template("help.html", topics=HELP_TOPICS, active_topic=topic, settings=_template_safe_settings(load_settings()))
@app.route("/settings")
def settings_page():
    settings = load_settings()
    display_settings = _template_safe_settings(settings_with_effective_workflow_paths(settings))
    for key in SECRET_SPECS.keys():
        display_settings[key] = masked_secret_value(key, settings)
        display_settings[key + "_source"] = secret_source(key, settings)
    share_destinations, share_destinations_editor, share_destinations_source = _display_share_destinations(settings)
    display_settings["share_destinations_editor"] = share_destinations_editor
    display_settings["share_destinations_source"] = share_destinations_source
    posting_providers, posting_providers_editor, posting_providers_source = _display_posting_providers(settings, display_settings)
    display_settings["posting_providers_editor"] = posting_providers_editor
    display_settings["posting_providers_source"] = posting_providers_source
    return render_template("settings.html", settings=display_settings, posting_providers=posting_providers, share_destinations=share_destinations, share_category_options=CATEGORY_KEY_OPTIONS)

@app.route("/plex")
def plex_page():
    return redirect(url_for("settings_page"))

@app.route("/clean")
def clean_page():
    return render_template("clean.html", settings=_template_safe_settings(load_settings()))

@app.route("/clean/logs")
def clean_logs_page():
    return render_template("clean_logs.html", logs=list_clean_actions(500))

@app.route("/api/settings/save", methods=["POST"])
def api_settings_save():
    current = load_settings()
    data = dict(current)
    for k in ["tv_root","movie_root","youtube_root","dest_root","end_tag","prepare_max_concurrent_jobs","prepare_permissions_mode","packing_max_concurrent_jobs","recycle_bin_root","plex_url","plex_token","plex_tv_library","plex_movie_library","plex_youtube_library","packing_watch_root","packing_output_root","posting_watch_root","packing_stability_delay","packing_password_prefix","packing_password_length","packing_par2_threads","packing_par2_memory_mb","packing_par2_block_size","packing_name_length","packing_name_fixed_tag","packing_name_fixed_pos","packing_thumbnail_host","packing_freeimage_api_key","posting_posted_root","posting_nzb_root","posting_article_size","posting_yenc_line_size","posting_retries","posting_retry_delay","posting_connection_headroom","posting_provider_failure_cooldown_seconds","posting_provider_disconnect_drain_seconds","posting_comment","posting_provider2_max_gb_when_busy","posting_provider1_host","posting_provider1_port","posting_provider1_username","posting_provider1_password","posting_provider1_connections","posting_provider1_max_connections","posting_provider2_host","posting_provider2_port","posting_provider2_username","posting_provider2_password","posting_provider2_connections","posting_provider2_max_connections","posting_providers_json","auth_username","github_repo_owner","github_repo_name","share_watch_root","share_import_root","share_request_timeout","share_destinations_json"]:
        if k in request.form:
            incoming = request.form.get(k, current.get(k, "")).strip()
            clear_requested = str(request.form.get(f"clear_{k}", "")).lower() in {"1", "true", "yes", "on"}
            if k in SECRET_SPECS:
                source = secret_source(k, current)
                if source in {"secret_file", "env_var"}:
                    if k not in {"posting_providers_json", "share_destinations_json"} and (
                        clear_requested or (incoming and not incoming.startswith("********"))
                    ):
                        flash(f"{k} is managed externally and was not overwritten.", "warning")
                    data[k] = current.get(k, "")
                    continue
                if clear_requested:
                    incoming = ""
                elif not incoming or incoming.startswith("********"):
                    incoming = current.get(k, "")
            data[k] = incoming
    submitted_username = str(data.get("auth_username", "") or "").strip()
    if not submitted_username or len(submitted_username) > 128:
        flash("Admin username must contain between 1 and 128 characters.", "error")
        return redirect(url_for("settings_page"))
    auth_identity_changed = not secrets.compare_digest(submitted_username, auth_username(current))
    data["auth_username"] = submitted_username
    data["clean_dry_run"] = "true" if request.form.get("clean_dry_run") else "false"
    data["clean_use_recycle_bin"] = "true" if request.form.get("clean_use_recycle_bin") else "false"
    data["packing_delete_source_after_success"] = "true" if request.form.get("packing_delete_source_after_success") else "false"
    data["packing_header_encrypt"] = "true" if request.form.get("packing_header_encrypt") else "false"
    data["packing_auto_volume"] = "true" if request.form.get("packing_auto_volume") else "false"
    data["packing_auto_par2"] = "true" if request.form.get("packing_auto_par2") else "false"
    data["posting_embed_password_in_nzb"] = "true" if request.form.get("posting_embed_password_in_nzb") else "false"
    data["posting_post_check"] = "true" if request.form.get("posting_post_check") else "false"
    data["workflow_auto_chain_enabled"] = "true" if request.form.get("workflow_auto_chain_enabled") else "false"
    data["update_check_enabled"] = "true" if request.form.get("update_check_enabled") else "false"
    data["share_auto_after_posting"] = "true" if request.form.get("share_auto_after_posting") else "false"

    provider_source = secret_source("posting_providers_json", current)
    if provider_source in {"secret_file", "env_var"}:
        data["posting_providers_json"] = current.get("posting_providers_json", "[]")
        normalized_providers = get_posting_providers(current)
        if "posting_providers_json" in request.form:
            flash("Posting providers are managed externally and were not overwritten.", "warning")
    else:
        try:
            raw_providers = request.form.get("posting_providers_json", data.get("posting_providers_json", "[]"))
            submitted_providers = json.loads(raw_providers or "[]")
            if not isinstance(submitted_providers, list) or len(submitted_providers) > 20:
                raise ValueError("posting_providers_json must be a list of at most 20 providers")
            provider_items = sanitize_posting_provider_items(submitted_providers)
            _validate_posting_providers(provider_items)
        except JsonSchemaValidationError as exc:
            flash(f"Posting providers validation failed: {exc.message}. Previous provider list was kept.", "warning")
            submitted_providers = get_posting_providers(current)
            provider_items = sanitize_posting_provider_items(submitted_providers)
        except Exception as exc:
            flash(f"Posting providers could not be parsed: {str(exc)[:200]}. Previous provider list was kept.", "warning")
            submitted_providers = get_posting_providers(current)
            provider_items = sanitize_posting_provider_items(submitted_providers)

        existing_providers = get_posting_providers(current)
        existing_by_id = {
            str(item.get("id") or "").strip().casefold(): item
            for item in existing_providers if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        normalized_providers = []
        for idx, item in enumerate(provider_items, start=1):
            if not isinstance(item, dict):
                continue
            submitted = submitted_providers[idx - 1] if idx <= len(submitted_providers) and isinstance(submitted_providers[idx - 1], dict) else {}
            merged = dict(_default_posting_provider(idx))
            merged.update(item)
            merged["id"] = _provider_slug(merged.get("id") or merged.get("name"), idx)
            merged["name"] = str(merged.get("name") or f"Provider {idx}").strip() or f"Provider {idx}"
            merged["enabled"] = bool(merged.get("enabled"))
            merged["ssl"] = bool(merged.get("ssl", True))
            merged["host"] = str(merged.get("host", "") or "").strip()
            merged["port"] = str(merged.get("port", "563") or "563").strip()
            merged["username"] = str(merged.get("username", "") or "").strip()
            merged["connections"] = str(merged.get("connections", "25") or "25").strip()
            merged["max_connections"] = str(merged.get("max_connections", merged.get("connections", "25")) or merged.get("connections", "25") or "25").strip()
            merged["account_group"] = _provider_slug(merged.get("account_group", ""), 0) if str(merged.get("account_group", "") or "").strip() else ""
            merged["priority_up_to_gb"] = str(merged.get("priority_up_to_gb", "0") or "0").strip() or "0"
            previous = existing_by_id.get(merged["id"].casefold())
            if previous is None and idx <= len(existing_providers):
                previous = existing_providers[idx - 1]
            password_value = str(merged.get("password", "") or "")
            clear_password = str(submitted.get("clear_password", "")).lower() in {"1", "true", "yes", "on"}
            if clear_password:
                password_value = ""
            elif not password_value or password_value.startswith("********"):
                password_value = str((previous or {}).get("password", "") or "")
            merged["password"] = password_value
            normalized_providers.append(merged)

        data["posting_providers_json"] = json.dumps(normalized_providers, ensure_ascii=False, indent=2)
        _sync_legacy_posting_provider_settings(data, normalized_providers)

    share_source = secret_source("share_destinations_json", current)
    if share_source in {"secret_file", "env_var"}:
        data["share_destinations_json"] = current.get("share_destinations_json", "[]")
        if "share_destinations_json" in request.form:
            flash("Share destinations are managed externally and were not overwritten.", "warning")
    else:
        existing_destinations = get_share_destinations(current)
        existing_by_id = {
            str(item.get("id") or "").strip().casefold(): item
            for item in existing_destinations if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        try:
            submitted_destinations = json.loads(request.form.get("share_destinations_json", data.get("share_destinations_json", "[]")) or "[]")
            if not isinstance(submitted_destinations, list) or len(submitted_destinations) > 50:
                raise ValueError("share_destinations_json must be a list of at most 50 destinations")
            normalized_destinations = []
            for idx, item in enumerate(submitted_destinations, start=1):
                if not isinstance(item, dict):
                    raise ValueError(f"Share destination {idx} must be an object")
                entry = {key: value for key, value in item.items() if key in _SHARE_DESTINATION_KEYS}
                entry["id"] = _provider_slug(entry.get("id") or entry.get("name"), idx)
                entry["name"] = str(entry.get("name") or f"Destination {idx}").strip()[:256] or f"Destination {idx}"
                entry["base_url"] = normalize_share_base_url(entry.get("base_url", ""))
                previous = existing_by_id.get(entry["id"].casefold())
                if previous is None and idx <= len(existing_destinations):
                    previous = existing_destinations[idx - 1]
                for secret_name in ("api_key", "password"):
                    incoming = str(entry.get(secret_name, "") or "")
                    clear_secret = str(item.get(f"clear_{secret_name}", "")).lower() in {"1", "true", "yes", "on"}
                    if clear_secret:
                        entry[secret_name] = ""
                    elif not incoming or incoming.startswith("********"):
                        entry[secret_name] = str((previous or {}).get(secret_name, "") or "")
                normalized_destinations.append(entry)
            data["share_destinations_json"] = json.dumps(normalized_destinations, ensure_ascii=False, indent=2)
        except Exception as exc:
            flash(f"Share destinations could not be saved: {str(exc)[:200]}. Previous destination list was kept.", "warning")
            data["share_destinations_json"] = current.get("share_destinations_json", "[]")

    data, warnings = normalize_settings(data)
    if auth_identity_changed:
        auth_result = update_auth_settings_atomic(
            {"auth_username": submitted_username},
            expected={
                "auth_username": auth_username(current),
                "auth_password_hash": auth_password_hash(current),
            },
        )
        if auth_result is None:
            flash("Account settings changed in another request. Reload and try again.", "warning")
            return redirect(url_for("settings_page"))
        data.update(auth_result)
        _establish_authenticated_session(auth_result, submitted_username)
    changed_settings = {
        key: value for key, value in data.items()
        if key not in AUTH_SETTING_KEYS and str(current.get(key, "")) != str(value)
    }
    save_settings_patch(changed_settings)
    try:
        start_watchers(data)
    except Exception as exc:
        LOG.warning("Unable to refresh file-system watchers after settings save: %s", exc)
    for warning in warnings:
        flash(warning, "warning")
    flash("Settings saved.", "success")
    return redirect(url_for("settings_page"))

@app.route("/api/plex/save", methods=["POST"])
def api_plex_save():
    current = load_settings()
    data = dict(current)
    plex_url = request.form.get("plex_url", "").strip()
    if plex_url:
        try:
            plex_url = normalize_service_base_url(plex_url)
        except Exception as exc:
            flash(f"Plex URL is invalid: {exc}", "error")
            return redirect(url_for("settings_page"))
    data["plex_url"] = plex_url
    submitted_token = request.form.get("plex_token", "").strip()
    clear_token = str(request.form.get("clear_plex_token", "")).lower() in {"1", "true", "yes", "on"}
    token_source = secret_source("plex_token", current)
    if token_source in {"secret_file", "env_var"}:
        if clear_token or (submitted_token and not submitted_token.startswith("********")):
            flash("The Plex token is managed externally and was not overwritten.", "warning")
    elif clear_token:
        data["plex_token"] = ""
    elif submitted_token and not submitted_token.startswith("********"):
        data["plex_token"] = submitted_token
    data["plex_tv_library"] = request.form.get("plex_tv_library","").strip()
    data["plex_movie_library"] = request.form.get("plex_movie_library","").strip()
    data["plex_youtube_library"] = request.form.get("plex_youtube_library","").strip()
    save_settings_patch({
        key: value for key, value in data.items()
        if key in {"plex_url", "plex_token", "plex_tv_library", "plex_movie_library", "plex_youtube_library"}
        and str(current.get(key, "")) != str(value)
    })
    flash("Plex settings saved.", "success")
    return redirect(url_for("plex_page"))


@app.route("/plex/signin")
def plex_signin():
    s = load_settings()
    if secret_source("plex_token", s) in {"secret_file", "env_var"}:
        flash("The Plex token is managed externally. Remove that external setting before using Plex sign-in.", "warning")
        return redirect(url_for("settings_page"))
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
    pending_pin_id = str(session.get("plex_pending_pin_id", "") or "")
    pin_id = str(request.args.get("pin_id", "") or pending_pin_id)
    if not pin_id:
        flash("Plex sign-in could not be completed because the PIN information was missing.", "error")
        return redirect(url_for("settings_page"))
    if not pending_pin_id or not secrets.compare_digest(pin_id, pending_pin_id):
        flash("Plex sign-in state did not match. Start the sign-in flow again.", "error")
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

    if secret_source("plex_token", s) in {"secret_file", "env_var"}:
        session.pop("plex_pending_pin_id", None)
        flash("The Plex token became externally managed during sign-in and was not saved.", "warning")
        return redirect(url_for("settings_page"))
    plex_patch = {"plex_token": token}
    if chosen_url:
        plex_patch["plex_url"] = chosen_url
    save_settings_patch(plex_patch)
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
    token = resolve_secret("plex_token", s)
    if not token:
        return jsonify({"ok": False, "error": "No Plex token saved yet."}), 400
    client_id = s.get("plex_client_id", "prepac-local-client")
    product = s.get("plex_product_name", "PrepaC")
    servers = list_servers_for_token(token, client_id, product)
    nonce = secrets.token_urlsafe(18)
    signing_key = str(app.secret_key or "").encode("utf-8")
    issued = 0
    for server in servers:
        filtered_connections = []
        for connection in server.get("connections", []) if isinstance(server, dict) else []:
            try:
                normalized_url = normalize_service_base_url(connection.get("uri", ""))
            except Exception:
                continue
            if issued >= 100:
                break
            connection["uri"] = normalized_url
            connection["choice_token"] = hmac.new(
                signing_key,
                f"plex-server-choice\0{nonce}\0{normalized_url}".encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            filtered_connections.append(connection)
            issued += 1
        if isinstance(server, dict):
            server["connections"] = filtered_connections
    session["plex_server_choice_nonce"] = nonce
    return jsonify({"ok": True, "servers": servers})

@app.route("/api/plex/server/select", methods=["POST"])
def api_plex_server_select():
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "A JSON object is required"}), 400
    try:
        server_url = normalize_service_base_url(data.get("server_url", "")) if isinstance(data, dict) else ""
    except Exception:
        server_url = ""
    if not server_url:
        return jsonify({"ok": False, "error": "server_url required"}), 400
    nonce = str(session.get("plex_server_choice_nonce", "") or "")
    supplied_token = str(data.get("choice_token", "") or "")
    expected_token = hmac.new(
        str(app.secret_key or "").encode("utf-8"),
        f"plex-server-choice\0{nonce}\0{server_url}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not nonce or not supplied_token or not hmac.compare_digest(supplied_token, expected_token):
        return jsonify({"ok": False, "error": "Refresh the Plex server list and select one of the returned connections."}), 400
    s = save_selected_server(server_url)
    session.pop("plex_server_choice_nonce", None)
    return jsonify({"ok": True, "plex_url": s.get("plex_url"), "token_saved": bool(resolve_secret("plex_token", s))})



@app.route("/api/local_image")
def api_local_image():
    path = (request.args.get("path") or "").strip()
    if not path:
        return ("", 404)
    try:
        settings = load_settings()
        config_root = CONFIG_DIR.resolve(strict=False)
        allowed_roots = [r for r in build_allowed_roots(settings) if pathlib.Path(r).resolve(strict=False) != config_root]
        assert_no_parent_traversal(path, "local image path")
        assert_path_within_roots(path, allowed_roots, "local image path")
    except Exception:
        return ("", 404)
    p = pathlib.Path(path)
    if not p.exists() or not p.is_file():
        return ("", 404)
    try:
        if p.stat().st_size > 10 * 1024 * 1024:
            return ("", 413)
    except OSError:
        return ("", 404)
    ext = p.suffix.lower()
    mime = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
    }.get(ext, "application/octet-stream")
    if mime == "application/octet-stream":
        return ("", 404)
    try:
        return send_file(p, mimetype=mime, conditional=True, max_age=0)
    except Exception:
        return ("", 404)


def _bounded_upstream_image(url, *, headers=None, params=None, max_bytes=8 * 1024 * 1024):
    response = requests.get(
        url,
        headers=headers or {},
        params=params,
        timeout=(5, 30),
        allow_redirects=False,
        stream=True,
    )
    try:
        if response.status_code != 200:
            return None
        content_type = str(response.headers.get("Content-Type", "") or "").split(";", 1)[0].strip().lower()
        if content_type not in {"image/jpeg", "image/png", "image/webp", "image/gif", "image/avif"}:
            return None
        try:
            declared_size = int(response.headers.get("Content-Length", "0") or 0)
        except Exception:
            declared_size = 0
        if declared_size > max_bytes:
            return None
        chunks = []
        size = 0
        iterator = response.iter_content(chunk_size=64 * 1024) if hasattr(response, "iter_content") else [getattr(response, "content", b"")]
        for chunk in iterator:
            if not chunk:
                continue
            size += len(chunk)
            if size > max_bytes:
                return None
            chunks.append(chunk)
        if not chunks:
            return None
        return Response(b"".join(chunks), mimetype=content_type, headers={"Cache-Control": "private, no-store"})
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()

@app.route("/api/plex/image")
def api_plex_image():
    settings = load_settings()
    plex_url = (settings.get("plex_url") or "").strip()
    plex_token = resolve_secret("plex_token", settings)
    path = (request.args.get("path") or "").strip()
    if not plex_url or not plex_token or not path or len(path) > 2048 or not path.startswith("/") or "://" in path:
        return ("", 404)

    try:
        plex_url = normalize_service_base_url(plex_url)
        width = min(1600, max(16, int(request.args.get("width", "400"))))
        height = min(1600, max(16, int(request.args.get("height", "600"))))
    except Exception:
        return ("", 400)

    headers = {"X-Plex-Token": plex_token}
    photo_url = f"{plex_url.rstrip('/')}/photo/:/transcode"
    params = {
        "url": path,
        "width": str(width),
        "height": str(height),
        "minSize": "1",
        "upscale": "1",
    }
    try:
        image_response = _bounded_upstream_image(photo_url, headers=headers, params=params)
        if image_response is not None:
            return image_response
    except Exception:
        pass

    try:
        image_response = _bounded_upstream_image(f"{plex_url.rstrip('/')}{path}", headers=headers)
        if image_response is not None:
            return image_response
    except Exception:
        pass

    return ("", 404)

@app.route("/api/clean/reset_prepared", methods=["POST"])
def api_clean_reset_prepared():
    try:
        data = request.get_json(silent=True) or {}
        try:
            prepared_item_id = int(data.get("prepared_item_id") or 0)
        except (TypeError, ValueError):
            prepared_item_id = 0
        if prepared_item_id <= 0:
            return jsonify({"ok": False, "error": "A valid prepared_item_id is required"}), 400

        removed = delete_prepared_by_id(prepared_item_id)

        if removed == 0:
            return jsonify({
                "ok": False,
                "error": "No prepared record was removed",
                "prepared_item_id": prepared_item_id,
            }), 404

        return jsonify({
            "ok": True,
            "removed": removed,
            "removed_by": "id",
            "prepared_item_id": prepared_item_id,
        })
    except Exception as exc:
        LOG.warning("Prepared record reset failed: %s", redact_sensitive_data(str(exc)))
        return jsonify({"ok": False, "error": "Failed to reset the prepared record"}), 500

@app.route("/prepare")
def prepare_page():
    return render_template("prepare.html", settings=_template_safe_settings(load_settings()))

@app.route("/prepare/tv")
def prepare_tv_page():
    return redirect(url_for("prepare_page"))

@app.route("/prepare/movie")
def prepare_movie_page():
    return redirect(url_for("prepare_page"))

@app.route("/jobs")
def jobs_page():
    return redirect(url_for("prepare_page"))



@app.route("/packing")
def packing_page():
    return render_template("packing.html", settings=_template_safe_settings(settings_with_effective_workflow_paths(load_settings())))

@app.route("/api/packing/scan", methods=["POST"])
def api_packing_scan():
    settings = load_settings()
    results = scan_watch_folder(settings)
    inc("prepac_scan_requests", 1, kind="packing")
    set_gauge("prepac_last_scan_results", len(results), kind="packing")
    return jsonify({"ok": True, "results": results})

@app.route("/api/packing/start", methods=["POST"])
def api_packing_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    data = request.get_json(silent=True) or {}
    try:
        candidate_ids = _bounded_string_list(data, "candidate_ids", max_items=200, max_length=256)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    settings = load_settings()
    if not candidate_ids:
        return jsonify({"ok": False, "error": "Select at least one packing candidate."}), 400
    try:
        candidates = [resolve_packing_candidate(candidate_id, settings) for candidate_id in candidate_ids]
    except Exception as exc:
        LOG.info("Packing candidate resolution rejected: %s", redact_sensitive_data(str(exc)))
        return jsonify({"ok": False, "error": "One or more packing candidates expired. Scan again and retry."}), 409
    if any(candidate is None for candidate in candidates):
        return jsonify({"ok": False, "error": "One or more packing candidates expired. Scan again and retry."}), 409
    started = []
    for candidate_id, candidate in zip(candidate_ids, candidates):
        started.append(start_packing_job_async(candidate["source_path"], settings, idempotency_key=f"packing:{candidate_id}"))
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
    return render_template("posting.html", settings=_template_safe_settings(settings_with_effective_workflow_paths(load_settings())))

@app.route("/api/posting/scan", methods=["POST"])
def api_posting_scan():
    settings = load_settings()
    results = scan_posting_candidates(settings)
    inc("prepac_scan_requests", 1, kind="posting")
    set_gauge("prepac_last_scan_results", len(results), kind="posting")
    return jsonify({"ok": True, "results": results})

@app.route("/api/posting/start", methods=["POST"])
def api_posting_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    data = request.get_json(silent=True) or {}
    try:
        candidate_ids = _bounded_string_list(data, "candidate_ids", max_items=200, max_length=256)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    settings = load_settings()
    if not candidate_ids:
        return jsonify({"ok": False, "error": "Select at least one posting candidate."}), 400
    try:
        candidates = [resolve_posting_candidate(candidate_id, settings) for candidate_id in candidate_ids]
    except Exception as exc:
        LOG.info("Posting candidate resolution rejected: %s", redact_sensitive_data(str(exc)))
        return jsonify({"ok": False, "error": "One or more posting candidates expired. Scan again and retry."}), 409
    if any(candidate is None for candidate in candidates):
        return jsonify({"ok": False, "error": "One or more posting candidates expired. Scan again and retry."}), 409
    started = []
    for candidate_id, candidate in zip(candidate_ids, candidates):
        started.append(start_posting_job_async(candidate["packed_root"], settings, idempotency_key=f"posting:{candidate_id}"))
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
        return jsonify({"ok": True, "raw_output": redact_sensitive_data(raw_output), "stats": _public_api_value(stats), "source": "memory"})
    jobs = list_posting_jobs(500)
    job = next((j for j in jobs if int(j.get("id", 0)) == int(job_id)), None)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    posted_root = job.get("posted_root") or str(posting_posted_root(load_settings()) / str(job.get("job_name", "")))
    log_path = pathlib.Path(posted_root) / "posting.log"
    return jsonify({"ok": True, "raw_output": redact_sensitive_data(_tail_text_file(str(log_path), 200)), "stats": _public_api_value(parse_posting_log_stats(str(log_path))), "source": "log"})

@app.route("/clean/result")
def clean_result_page():
    return render_template("clean_result.html")


@app.route("/share")
def share_page():
    settings = load_settings()
    return render_template("share.html", settings=_template_safe_settings(settings_with_effective_workflow_paths(settings)), category_options=CATEGORY_KEY_OPTIONS)

@app.route("/api/share/candidates")
def api_share_candidates():
    settings = load_settings()
    destinations = public_share_destinations(settings)
    results = build_share_candidates(settings)
    for item in results:
        item["resolved_categories"] = build_resolved_category_preview(destinations, item.get("category_key") or "movie_hd")
    return jsonify({"ok": True, "results": results, "destinations": destinations, "category_options": CATEGORY_KEY_OPTIONS})

@app.route("/api/share/jobs")
def api_share_jobs():
    return jsonify(_share_jobs_payload())

@app.route("/api/share/jobs/stream")
def api_share_jobs_stream():
    return _event_stream(_share_jobs_payload)


@app.route("/api/share/review", methods=["POST"])
def api_share_review():
    payload = request.get_json(silent=True) or {}
    try:
        items, destination_ids = _bounded_share_submission(payload)
    except (TypeError, ValueError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    review = build_share_submission_review(items, destination_ids, load_settings())
    return jsonify({"ok": True, **review})

@app.route("/api/share/start", methods=["POST"])
def api_share_start():
    draining = reject_if_draining()
    if draining:
        return draining
    payload = request.get_json(silent=True) or {}
    try:
        items, destination_ids = _bounded_share_submission(payload)
    except (TypeError, ValueError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    result = queue_share_jobs(items, destination_ids, load_settings())
    return jsonify({"ok": True, "state": "accepted", **result}), 202

@app.route("/api/share/retry", methods=["POST"])
def api_share_retry():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    payload = request.get_json(silent=True) or {}
    force_outcome_unknown = payload.get("force_outcome_unknown", False)
    if not isinstance(force_outcome_unknown, bool):
        return jsonify({"ok": False, "error": "force_outcome_unknown must be a JSON boolean"}), 400
    try:
        job_id = int(payload.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if job_id <= 0:
        return jsonify({"ok": False, "error": "A valid job_id is required"}), 400
    job = get_share_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Share job not found"}), 404
    if force_outcome_unknown:
        if not force_retry_share_outcome_unknown(job_id):
            return jsonify({"ok": False, "error": "Only outcome_unknown share jobs can be force retried"}), 409
    elif not increment_share_retry(job_id):
        return jsonify({"ok": False, "error": "Only failed share jobs can be retried normally"}), 409
    start_share_job_async(job_id, load_settings())
    response = {"ok": True, "job_id": job_id, "forced": force_outcome_unknown}
    if force_outcome_unknown:
        response["warning"] = "The prior upload outcome was unknown; this retry may create a duplicate at the destination."
    return jsonify(response)

@app.route("/api/share/cancel", methods=["POST"])
def api_share_cancel():
    payload = request.get_json(silent=True) or {}
    try:
        job_id = int(payload.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if not job_id:
        return jsonify({"ok": False, "error": "job_id is required"}), 400
    changed = cancel_share_job(job_id, reason="Cancelled by user")
    if not changed:
        return jsonify({"ok": False, "error": "Share job could not be cancelled"}), 400
    return jsonify({"ok": True, "job_id": job_id})

@app.route("/api/share/import", methods=["POST"])
def api_share_import():
    nzb_rar_file = request.files.get("nzb_rar")
    template_file = request.files.get("template_file")
    mediainfo_file = request.files.get("mediainfo_file")
    release_name = (request.form.get("release_name", "") or "").strip()
    if not nzb_rar_file or not template_file:
        return jsonify({"ok": False, "error": "NZB RAR and template file are required"}), 400
    if len(release_name) > 512:
        return jsonify({"ok": False, "error": "release_name is limited to 512 characters"}), 400
    bundle_id = import_share_bundle(nzb_rar_file, template_file, mediainfo_file, release_name)
    return jsonify({"ok": True, "bundle_id": bundle_id})

@app.route("/api/share/import-bulk", methods=["POST"])
def api_share_import_bulk():
    nzb_rar_files = request.files.getlist("nzb_rar_files")
    template_files = request.files.getlist("template_files")
    mediainfo_files = request.files.getlist("mediainfo_files")
    if not nzb_rar_files or not template_files:
        return jsonify({"ok": False, "error": "RARred NZBs and template files are required"}), 400
    if len(nzb_rar_files) > 100 or len(template_files) > 100 or len(mediainfo_files) > 100:
        return jsonify({"ok": False, "error": "Bulk imports are limited to 100 files per field"}), 400
    result = import_share_bundles_bulk(nzb_rar_files, template_files, mediainfo_files)
    return jsonify({"ok": True, **result})

@app.route("/api/share/candidate/remove", methods=["POST"])
def api_share_candidate_remove():
    payload = request.get_json(silent=True) or {}
    candidate_id = str(payload.get("candidate_id") or "").strip()
    if not candidate_id or len(candidate_id) > 256:
        return jsonify({"ok": False, "error": "candidate_id is required"}), 400
    result = remove_share_candidate(candidate_id, load_settings())
    return jsonify({"ok": True, **result})

@app.route("/api/share/caps/refresh", methods=["POST"])
def api_share_caps_refresh():
    return jsonify({"ok": True, "results": refresh_share_caps(load_settings())})

@app.route("/api/share/destination/test", methods=["POST"])
def api_share_destination_test():
    payload = request.get_json(silent=True) or {}
    destination_id = str(payload.get("destination_id") or "").strip() if isinstance(payload, dict) else ""
    if not destination_id or len(destination_id) > 256:
        return jsonify({"ok": False, "error": "A saved destination_id is required"}), 400
    destination = next((item for item in get_share_destinations(load_settings()) if str(item.get("id") or "") == destination_id), None)
    if destination is None:
        return jsonify({"ok": False, "error": "Saved destination not found"}), 404
    try:
        categories = fetch_destination_caps(destination, timeout=15)
        return jsonify({"ok": True, "count": len(categories), "categories": categories[:50]})
    except Exception as exc:
        LOG.warning("Share destination test failed: %s", redact_sensitive_data(str(exc)))
        return jsonify({"ok": False, "error": "Destination capabilities request failed"}), 400

@app.route("/history")
def history_page():
    return render_template("history_index.html")

@app.route("/history/prepare")
def history_prepare_page():
    return render_template("history.html", history=enrich_prepare_history_rows(list_history(500), list_jobs(500)), prepare_jobs=_prepare_history_jobs(500))

@app.route("/history/clean")
def history_clean_page():
    return render_template("clean_logs.html", logs=list_clean_actions(500))

@app.route("/history/packing")
def history_packing_page():
    jobs = enrich_packing_history_rows(list_packing_history(500))
    return render_template("packing_history.html", jobs=[_public_api_value(job) for job in jobs])

@app.route("/history/posting")
def history_posting_page():
    return render_template("posting_history.html", jobs=[_public_api_value(job) for job in list_posting_history(500)])

@app.route("/history/share")
def history_share_page():
    return render_template("share_history.html", jobs=[_public_api_value(job) for job in list_share_history(500)])


def _csv_safe_cell(value):
    if not isinstance(value, str):
        return value
    stripped = value.lstrip()
    if stripped.startswith(("=", "+", "-", "@")) or value.startswith(("\t", "\r", "\n")):
        return "'" + value
    return value


def _csv_safe_row(values):
    return [_csv_safe_cell(value) for value in values]

@app.route("/api/history/export.csv")
def api_history_export():
    rows = list_history(5000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","media_type","source_path","source_rel","dest_path","source_bytes","dest_bytes","chosen_bracket","end_tag","created_at"])
    for r in rows:
        writer.writerow(_csv_safe_row([r.get("id"), r.get("media_type"), r.get("source_path"), r.get("source_rel"), r.get("dest_path"), r.get("source_bytes"), r.get("dest_bytes"), r.get("chosen_bracket"), r.get("end_tag"), r.get("created_at")]))
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_history.csv"})

@app.route("/api/clean/logs/export.csv")
def api_clean_logs_export():
    rows = list_clean_actions(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","reason","media_type","target_path","target_kind","dry_run","success","size_bytes","message"])
    for r in rows:
        writer.writerow(_csv_safe_row([r.get("id"), r.get("created_at"), r.get("reason"), r.get("media_type"), r.get("target_path"), r.get("target_kind"), r.get("dry_run"), r.get("success"), r.get("size_bytes"), r.get("message")]))
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_clean_logs.csv"})


@app.route("/api/packing/history/export.csv")
def api_packing_history_export():
    rows = list_packing_history(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","started_at","finished_at","job_name","source_path","output_root","status","phase","percent","size_bytes","rar_parts_estimate","par2_percent","archive_token","message"])
    for r in rows:
        writer.writerow(_csv_safe_row([r.get("id"), r.get("created_at"), r.get("started_at"), r.get("finished_at"), r.get("job_name"), r.get("source_path"), r.get("output_root"), r.get("status"), r.get("phase"), r.get("percent"), r.get("size_bytes"), r.get("rar_parts_estimate"), r.get("par2_percent"), r.get("archive_token"), r.get("message")]))
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_packing_history.csv"})

@app.route("/api/posting/history/export.csv")
def api_posting_history_export():
    rows = list_posting_history(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","started_at","finished_at","job_name","packed_root","posted_root","status","phase","percent","size_bytes","provider_used","nzb_path","message"])
    for r in rows:
        writer.writerow(_csv_safe_row([r.get("id"), r.get("created_at"), r.get("started_at"), r.get("finished_at"), r.get("job_name"), r.get("packed_root"), r.get("posted_root"), r.get("status"), r.get("phase"), r.get("percent"), r.get("size_bytes"), r.get("provider_used"), r.get("nzb_path"), r.get("message")]))
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_posting_history.csv"})


@app.route("/api/share/history/export.csv")
def api_share_history_export():
    rows = list_share_history(10000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id","created_at","started_at","finished_at","job_name","destination_name","selected_category_label","status","remote_id","remote_guid","message"])
    for r in rows:
        writer.writerow(_csv_safe_row([r.get("id"), r.get("created_at"), r.get("started_at"), r.get("finished_at"), r.get("job_name"), r.get("destination_name"), r.get("selected_category_label"), r.get("status"), r.get("remote_id"), r.get("remote_guid"), r.get("message")]))
    return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=prepac_share_history.csv"})

@app.route("/api/prepare/tv/search")
def api_prepare_tv_search():
    settings = load_settings()
    query = str(request.args.get("q", "") or "").strip()
    if len(query) > 256:
        return jsonify({"ok": False, "error": "Search query is limited to 256 characters"}), 400
    names = search_shows(settings["tv_root"], query)
    plex_posters = {x["name"]: x.get("poster_url","") for x in search_posters_for_prepare(settings, "tv", query)}
    results = []
    for n in names:
        local = show_poster(settings["tv_root"], n)
        results.append({"name": n, "poster_url": local or plex_posters.get(n, "")})
    return jsonify({"results": results})

@app.route("/api/prepare/tv/seasons")
def api_prepare_tv_seasons():
    show_name = str(request.args.get("show", "") or "").strip()
    if not show_name or len(show_name) > 512:
        return jsonify({"ok": False, "error": "A valid show name is required"}), 400
    return jsonify({"results": list_seasons(load_settings()["tv_root"], show_name)})

@app.route("/api/prepare/tv/preview", methods=["POST"])
def api_prepare_tv_preview():
    data = request.get_json(silent=True) or {}
    show_name = str(data.get("show_name") or "").strip()
    season_name = str(data.get("season_name") or "").strip()
    bracket_override = str(data.get("bracket_override") or data.get("chosen_bracket") or "").strip()
    if not show_name or not season_name or len(show_name) > 512 or len(season_name) > 256 or len(bracket_override) > 256:
        return jsonify({"ok": False, "error": "show_name and season_name are required"}), 400
    try:
        payload = preview_tv(load_settings(), show_name, season_name, bracket_override)
        payload["preview_token"] = _prepare_preview_token(payload)
        payload["ok"] = True
        return jsonify(payload)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/prepare/start", methods=["POST"])
def api_prepare_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "A JSON object is required."}), 400
    items = data.get("items")
    if not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "At least one Prepare item is required."}), 400
    if len(items) > _PREPARE_BATCH_LIMIT:
        return jsonify({
            "ok": False,
            "error": f"A maximum of {_PREPARE_BATCH_LIMIT} Prepare items can be queued at once.",
        }), 400

    try:
        settings = load_settings()
    except Exception as exc:
        return _prepare_queue_unavailable(exc, "batch")
    results = []
    launches = []
    for index, item in enumerate(items):
        media_type = str(item.get("kind") or "").strip().lower() if isinstance(item, dict) else ""
        try:
            result, launch = _create_prepare_queue_entry(settings, media_type, item)
            result["index"] = index
            results.append(result)
            if launch:
                launches.append((result, launch))
        except ValueError as exc:
            results.append({
                "index": index,
                "ok": False,
                "error": redact_sensitive_data(str(exc))[:300],
            })
        except Exception as exc:
            LOG.error(
                "Unable to persist Prepare batch item %s (%s): %s",
                index,
                type(exc).__name__,
                redact_sensitive_data(str(exc))[:300],
                exc_info=True,
            )
            results.append({
                "index": index,
                "ok": False,
                "error": "Prepare queue is temporarily unavailable. Try again in a few seconds.",
            })

    for result, launch in launches:
        if not _launch_prepare_worker(*launch):
            result.update({
                "ok": False,
                "duplicate": False,
                "error": "Prepare worker could not be started. Submit this item again.",
            })

    queued = sum(1 for item in results if item.get("ok") and not item.get("duplicate"))
    duplicates = sum(1 for item in results if item.get("ok") and item.get("duplicate"))
    failed = sum(1 for item in results if not item.get("ok"))
    response = {
        "ok": True,
        "requested": len(items),
        "queued": queued,
        "duplicates": duplicates,
        "failed": failed,
        "results": results,
    }
    return jsonify(response), 202 if queued else 200


@app.route("/api/prepare/tv/start", methods=["POST"])
def api_prepare_tv_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    payload = request.get_json(silent=True) or {}
    show_name = str(payload.get("show_name") or "").strip()
    season_name = str(payload.get("season_name") or "").strip()
    bracket_override = str(payload.get("bracket_override") or payload.get("chosen_bracket") or "").strip()
    if not show_name or not season_name or len(show_name) > 512 or len(season_name) > 256 or len(bracket_override) > 256:
        return jsonify({"ok": False, "error": "Valid show_name and season_name values are required."}), 400
    settings = load_settings()
    try:
        server_preview = preview_tv(settings, show_name, season_name, bracket_override)
    except Exception as exc:
        return jsonify({"ok": False, "error": redact_sensitive_data(str(exc))[:300]}), 400
    source_path = str(server_preview.get("source_path") or "")
    dest_path = str(server_preview.get("dest_path") or "")
    if not source_path or not dest_path:
        return jsonify({"ok": False, "error": "The selected TV item could not be resolved. Rebuild the preview."}), 409
    idempotency_key = "prepare:tv:" + hashlib.sha256(f"{source_path}\0{dest_path}".encode("utf-8")).hexdigest()
    try:
        job_id, created = create_job(
            "tv",
            source_path,
            dest_path,
            idempotency_key=idempotency_key,
            return_created=True,
            initial_event=("queued", "TV prepare job queued.", 0),
        )
    except Exception as exc:
        return _prepare_queue_unavailable(exc, "TV")
    if not created:
        return jsonify({"ok": True, "job_id": job_id, "duplicate": True})
    worker_payload = dict(server_preview)
    worker_payload.update({"show_name": show_name, "season_name": season_name, "bracket_override": bracket_override})
    if not _launch_prepare_worker(job_id, "tv", settings, worker_payload):
        return jsonify({
            "ok": False,
            "error": "Prepare worker could not be started. Submit this item again.",
        }), 503
    return jsonify({"ok": True, "job_id": job_id})

@app.route("/api/prepare/movie/search")
def api_prepare_movie_search():
    settings = load_settings()
    query = str(request.args.get("q", "") or "").strip()
    if len(query) > 256:
        return jsonify({"ok": False, "error": "Search query is limited to 256 characters"}), 400
    names = search_movies(settings["movie_root"], query)
    plex_posters = {x["name"]: x.get("poster_url","") for x in search_posters_for_prepare(settings, "movie", query)}
    results = []
    for n in names:
        local = movie_poster(settings["movie_root"], n)
        results.append({"name": n, "poster_url": local or plex_posters.get(n, "")})
    return jsonify({"results": results})

@app.route("/api/prepare/movie/preview", methods=["POST"])
def api_prepare_movie_preview():
    data = request.get_json(silent=True) or {}
    movie_name = str(data.get("movie_name") or "").strip()
    bracket_override = str(data.get("bracket_override") or data.get("chosen_bracket") or "").strip()
    if not movie_name or len(movie_name) > 512 or len(bracket_override) > 256:
        return jsonify({"ok": False, "error": "movie_name is required"}), 400
    try:
        payload = preview_movie(load_settings(), movie_name, bracket_override)
        payload["preview_token"] = _prepare_preview_token(payload)
        payload["ok"] = True
        return jsonify(payload)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/api/prepare/movie/start", methods=["POST"])
def api_prepare_movie_start():
    blocked = reject_if_draining()
    if blocked:
        return blocked
    payload = request.get_json(silent=True) or {}
    movie_name = str(payload.get("movie_name") or "").strip()
    bracket_override = str(payload.get("bracket_override") or payload.get("chosen_bracket") or "").strip()
    if not movie_name or len(movie_name) > 512 or len(bracket_override) > 256:
        return jsonify({"ok": False, "error": "A valid movie_name is required."}), 400
    settings = load_settings()
    try:
        server_preview = preview_movie(settings, movie_name, bracket_override)
    except Exception as exc:
        return jsonify({"ok": False, "error": redact_sensitive_data(str(exc))[:300]}), 400
    source_path = str(server_preview.get("source_path") or "")
    dest_path = str(server_preview.get("dest_path") or "")
    if not source_path or not dest_path:
        return jsonify({"ok": False, "error": "The selected movie could not be resolved. Rebuild the preview."}), 409
    idempotency_key = "prepare:movie:" + hashlib.sha256(f"{source_path}\0{dest_path}".encode("utf-8")).hexdigest()
    try:
        job_id, created = create_job(
            "movie",
            source_path,
            dest_path,
            idempotency_key=idempotency_key,
            return_created=True,
            initial_event=("queued", "Movie prepare job queued.", 0),
        )
    except Exception as exc:
        return _prepare_queue_unavailable(exc, "Movie")
    if not created:
        return jsonify({"ok": True, "job_id": job_id, "duplicate": True})
    worker_payload = dict(server_preview)
    worker_payload.update({"movie_name": movie_name, "bracket_override": bracket_override})
    if not _launch_prepare_worker(job_id, "movie", settings, worker_payload):
        return jsonify({
            "ok": False,
            "error": "Prepare worker could not be started. Submit this item again.",
        }), 503
    return jsonify({"ok": True, "job_id": job_id})

@app.route("/api/prepare/cancel", methods=["POST"])
def api_prepare_cancel():
    data = request.get_json(silent=True) or {}
    try:
        job_id = int(data.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if not job_id:
        return jsonify({"ok": False, "error": "job_id required"}), 400
    changed = cancel_prepare_job(job_id)
    return jsonify({"ok": bool(changed)})


@app.route("/api/prepare/outcome-unknown/acknowledge", methods=["POST"])
def api_prepare_outcome_unknown_acknowledge():
    return _acknowledge_ambiguous_outcome(
        "Prepare",
        acknowledge_prepare_outcome_unknown_for_resubmission,
    )


@app.route("/api/packing/cancel", methods=["POST"])
def api_packing_cancel():
    data = request.get_json(silent=True) or {}
    try:
        job_id = int(data.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if not job_id:
        return jsonify({"ok": False, "error": "job_id required"}), 400
    changed = cancel_packing_job(job_id)
    if not changed:
        return jsonify({
            "ok": False,
            "error": (
                "Packing can only be cancelled before it claims and clears its "
                "output directories"
            ),
        }), 409
    return jsonify({"ok": True})


@app.route("/api/packing/outcome-unknown/acknowledge", methods=["POST"])
def api_packing_outcome_unknown_acknowledge():
    return _acknowledge_ambiguous_outcome(
        "Packing",
        acknowledge_packing_outcome_unknown_for_resubmission,
    )


@app.route("/api/posting/cancel", methods=["POST"])
def api_posting_cancel():
    data = request.get_json(silent=True) or {}
    try:
        job_id = int(data.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if not job_id:
        return jsonify({"ok": False, "error": "job_id required"}), 400
    changed = cancel_posting_job(job_id)
    return jsonify({"ok": bool(changed)})


@app.route("/api/posting/outcome-unknown/acknowledge", methods=["POST"])
def api_posting_outcome_unknown_acknowledge():
    return _acknowledge_ambiguous_outcome(
        "Posting",
        acknowledge_posting_outcome_unknown_for_resubmission,
    )


@app.route("/api/jobs")
def api_jobs():
    payload = _prepare_active_jobs_payload()
    return jsonify(payload), 200 if payload.get("ok") else 503

@app.route("/api/jobs/stream")
def api_jobs_stream():
    return _event_stream(_prepare_active_jobs_payload)

@app.route("/api/clean/preview")
def api_clean_preview():
    settings = load_settings()
    history = list_history(500)
    try:
        filter_reason, filter_type = normalize_clean_filter_scope(
            request.args.get("reason", "both"),
            request.args.get("type", "all"),
        )
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
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
            primary["candidate_id"] = clean_candidate_id(primary)
            collapsed.append(primary)
        else:
            collapsed.extend(items)

    collapsed.extend(passthrough)
    return collapsed

@app.route("/api/clean/delete", methods=["POST"])
def api_clean_delete():
    try:
        settings = load_settings()
        data = request.get_json(silent=True) or {}
        confirmation = data.get("confirmation", "")
        try:
            candidate_ids = _bounded_string_list(data, "candidate_ids", max_items=500, max_length=128)
            if "filter_reason" not in data or "filter_type" not in data:
                raise ValueError("Clean preview scope is required. Refresh the page and run the scan again.")
            filter_reason, filter_type = normalize_clean_filter_scope(
                data.get("filter_reason"),
                data.get("filter_type"),
            )
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        if "dry_run" in data and not isinstance(data.get("dry_run"), bool):
            return jsonify({"ok": False, "error": "dry_run must be a boolean"}), 400
        if "use_recycle_bin" in data and not isinstance(data.get("use_recycle_bin"), bool):
            return jsonify({"ok": False, "error": "use_recycle_bin must be a boolean"}), 400
        dry_run = data.get("dry_run", settings.get("clean_dry_run","true") == "true")
        use_recycle_bin = data.get("use_recycle_bin", settings.get("clean_use_recycle_bin","true") == "true")
        recycle_bin_root = settings.get("recycle_bin_root", "/media/dest/.prepac_recycle")
        if confirmation != "DELETE":
            return jsonify({"ok": False, "error": "Confirmation must equal DELETE."}), 400
        candidates = resolve_clean_candidate_ids(
            candidate_ids,
            settings,
            list_history(500),
            filter_reason,
            filter_type,
        )
        if len(candidates) != len(set(candidate_ids)):
            return jsonify({"ok": False, "error": "One or more clean candidates expired. Refresh the preview and retry."}), 409

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
