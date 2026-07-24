import json
from urllib.parse import quote, urlsplit

import requests

from app.plex_auth import normalize_plex_connection_uri


PLEX_CONNECT_TIMEOUT_SECONDS = 5
PLEX_READ_TIMEOUT_SECONDS = 20
PLEX_REQUEST_TIMEOUT = (PLEX_CONNECT_TIMEOUT_SECONDS, PLEX_READ_TIMEOUT_SECONDS)
MAX_PLEX_LIBRARY_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_PLEX_REFRESH_RESPONSE_BYTES = 256 * 1024
_STREAM_CHUNK_BYTES = 64 * 1024


def _plex_endpoint_url(base_url, path):
    """Build an endpoint that is guaranteed to remain on the configured origin."""
    base = normalize_plex_connection_uri(base_url)
    endpoint = str(path or "")
    if (
        not endpoint.startswith("/")
        or endpoint.startswith("//")
        or len(endpoint) > 4096
        or any(ord(ch) < 32 for ch in endpoint)
        or any(ch.isspace() for ch in endpoint)
        or any(ch in endpoint for ch in "\\?#")
    ):
        raise ValueError("Plex endpoint path is invalid")

    target = f"{base.rstrip('/')}{endpoint}"
    base_parts = urlsplit(base)
    target_parts = urlsplit(target)
    base_origin = (
        base_parts.scheme,
        str(base_parts.hostname or "").lower().rstrip("."),
        base_parts.port or (443 if base_parts.scheme == "https" else 80),
    )
    target_origin = (
        target_parts.scheme,
        str(target_parts.hostname or "").lower().rstrip("."),
        target_parts.port or (443 if target_parts.scheme == "https" else 80),
    )
    if target_origin != base_origin:
        raise ValueError("Plex endpoint must use the configured server origin")
    return target


def plex_section_path(section_key, suffix):
    key = str(section_key or "").strip()
    if not key or len(key) > 128 or any(ord(ch) < 32 for ch in key):
        raise ValueError("Plex library section key is invalid")
    return f"/library/sections/{quote(key, safe='')}/{suffix.lstrip('/')}"


def _bounded_response_bytes(response, max_bytes):
    headers = getattr(response, "headers", {}) or {}
    try:
        content_length = int(str(headers.get("Content-Length", "") or ""))
    except (TypeError, ValueError):
        content_length = None
    if content_length is not None and (content_length < 0 or content_length > max_bytes):
        raise RuntimeError("Plex response exceeded the allowed size")

    iterator_method = getattr(type(response), "iter_content", None)
    if callable(iterator_method):
        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=_STREAM_CHUNK_BYTES):
            if not chunk:
                continue
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            total += len(chunk)
            if total > max_bytes:
                raise RuntimeError("Plex response exceeded the allowed size")
            chunks.append(chunk)
        return b"".join(chunks)

    content = getattr(response, "content", b"")
    if isinstance(content, str):
        content = content.encode("utf-8")
    if len(content) > max_bytes:
        raise RuntimeError("Plex response exceeded the allowed size")
    return bytes(content)


def _close_response(response):
    close = getattr(response, "close", None)
    if callable(close):
        close()


def request_plex_bytes(
    base_url,
    token,
    path,
    *,
    params=None,
    timeout=PLEX_REQUEST_TIMEOUT,
    max_bytes=MAX_PLEX_LIBRARY_RESPONSE_BYTES,
):
    """Make one bounded request without ever forwarding credentials to a redirect."""
    target = _plex_endpoint_url(base_url, path)
    headers = {"X-Plex-Token": str(token), "Accept": "application/json"}
    response = requests.get(
        target,
        headers=headers,
        params=params or {},
        timeout=timeout,
        allow_redirects=False,
        stream=True,
    )
    try:
        status_code = int(response.status_code)
        if 300 <= status_code < 400:
            raise RuntimeError(
                "Plex redirects are not allowed; configure the final Plex server URL"
            )
        response.raise_for_status()
        return _bounded_response_bytes(response, max_bytes)
    finally:
        _close_response(response)


def request_plex_json(base_url, token, path, *, params=None):
    raw = request_plex_bytes(
        base_url,
        token,
        path,
        params=params,
        max_bytes=MAX_PLEX_LIBRARY_RESPONSE_BYTES,
    )
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Plex returned an invalid JSON response") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Plex returned an invalid JSON response")
    return data
