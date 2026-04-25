import secrets
from urllib.parse import urlsplit, urlunsplit


UNSAFE_HTTP_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
DEFAULT_SHARE_IMPORT_MAX_MIB = 512


def is_unsafe_http_method(method):
    return str(method or "").strip().upper() in UNSAFE_HTTP_METHODS


def ensure_csrf_token(session_obj, key="csrf_token"):
    token = str(session_obj.get(key, "") or "").strip()
    if not token:
        token = secrets.token_urlsafe(32)
        session_obj[key] = token
    return token


def csrf_token_matches(session_obj, provided, key="csrf_token"):
    expected = str(session_obj.get(key, "") or "").strip()
    candidate = str(provided or "").strip()
    return bool(expected and candidate) and secrets.compare_digest(expected, candidate)


def normalize_service_base_url(value):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Destination base_url is required")

    parts = urlsplit(raw)
    scheme = str(parts.scheme or "").lower()
    if scheme not in {"http", "https"}:
        raise ValueError("Destination base_url must start with http:// or https://")
    if not parts.hostname:
        raise ValueError("Destination base_url must include a hostname")
    if parts.username or parts.password:
        raise ValueError("Destination base_url must not include embedded credentials")
    if parts.query or parts.fragment:
        raise ValueError("Destination base_url must not include query strings or fragments")

    path = parts.path.rstrip("/")
    return urlunsplit((scheme, parts.netloc, path, "", ""))


def _first_forwarded_value(value):
    return str(value or "").split(",")[0].strip()


def build_external_base_url(request_scheme, request_host, forwarded_proto="", forwarded_host="", trust_proxy=False):
    proto = str(request_scheme or "http").strip().lower() or "http"
    host = str(request_host or "").strip()

    if trust_proxy:
        candidate_proto = _first_forwarded_value(forwarded_proto).lower()
        if candidate_proto in {"http", "https"}:
            proto = candidate_proto
        candidate_host = _first_forwarded_value(forwarded_host)
        if candidate_host:
            host = candidate_host

    return f"{proto}://{host}".rstrip("/")


def _positive_int(value, default):
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except Exception:
        return default
    return parsed if parsed > 0 else default


def share_import_limit_mebibytes(value=None, default_mib=DEFAULT_SHARE_IMPORT_MAX_MIB):
    return _positive_int(value, default_mib)


def share_import_limit_bytes(value=None, default_mib=DEFAULT_SHARE_IMPORT_MAX_MIB):
    return share_import_limit_mebibytes(value, default_mib) * 1024 * 1024
