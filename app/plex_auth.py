
import xml.etree.ElementTree as ET
import ipaddress
import json
from datetime import datetime
from urllib.parse import urlencode, urlsplit, urlunsplit
import requests
from app.db import save_pin, update_pin_status, save_settings_patch, load_settings

PLEX_TV = "https://plex.tv"
APP_PLEX = "https://app.plex.tv"
MAX_PLEX_RESOURCE_BYTES = 2 * 1024 * 1024
MAX_PLEX_PIN_BYTES = 256 * 1024
_STREAM_CHUNK_BYTES = 64 * 1024


def _bounded_response_bytes(response, limit=MAX_PLEX_RESOURCE_BYTES):
    headers = getattr(response, "headers", {}) or {}
    try:
        content_length = int(str(headers.get("Content-Length", "") or ""))
    except (TypeError, ValueError):
        content_length = None
    if content_length is not None and content_length > limit:
        raise RuntimeError("Plex response exceeded the allowed size")

    # Looking the method up on the type keeps simple MagicMock responses (which
    # synthesize arbitrary attributes) compatible with the content fallback.
    iterator_method = getattr(type(response), "iter_content", None)
    if callable(iterator_method):
        chunks = []
        size = 0
        for chunk in response.iter_content(chunk_size=_STREAM_CHUNK_BYTES):
            if not chunk:
                continue
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            size += len(chunk)
            if size > limit:
                raise RuntimeError("Plex response exceeded the allowed size")
            chunks.append(chunk)
        return b"".join(chunks)

    value = getattr(response, "content", b"")
    if isinstance(value, str):
        value = value.encode("utf-8")
    if len(value) > limit:
        raise RuntimeError("Plex response exceeded the allowed size")
    return bytes(value)


def _close_response(response):
    closer = getattr(response, "close", None)
    if callable(closer):
        closer()


def _response_encoding(response):
    value = getattr(response, "encoding", None)
    return value.strip() if isinstance(value, str) and value.strip() else "utf-8"


def _decode_json_response(response, limit):
    raw = _bounded_response_bytes(response, limit)
    encoding = _response_encoding(response)
    data = json.loads(raw.decode(encoding, errors="strict"))
    if not isinstance(data, dict):
        raise RuntimeError("Plex returned an invalid JSON response")
    return data


def normalize_plex_connection_uri(value):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Plex connection URI is required")
    if any(ord(ch) < 32 for ch in raw) or "\\" in raw or any(ch.isspace() for ch in raw):
        raise ValueError("Plex connection URI contains invalid characters")

    parts = urlsplit(raw)
    scheme = str(parts.scheme or "").lower()
    if scheme not in {"http", "https"}:
        raise ValueError("Plex connection URI must use http or https")
    if not parts.hostname:
        raise ValueError("Plex connection URI must include a hostname")
    if parts.username is not None or parts.password is not None:
        raise ValueError("Plex connection URI must not include credentials")
    if parts.query or parts.fragment:
        raise ValueError("Plex connection URI must not include a query or fragment")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError("Plex connection URI contains an invalid port") from exc
    if port is not None and not (1 <= port <= 65535):
        raise ValueError("Plex connection URI contains an invalid port")

    hostname = parts.hostname.lower().rstrip(".")
    if not hostname:
        raise ValueError("Plex connection URI must include a hostname")
    normalized_host = f"[{hostname}]" if ":" in hostname else hostname
    netloc = f"{normalized_host}:{port}" if port is not None else normalized_host
    path = parts.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def _is_loopback_connection(uri):
    hostname = str(urlsplit(uri).hostname or "").lower().rstrip(".")
    if hostname == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname.split("%", 1)[0]).is_loopback
    except ValueError:
        return False

def plex_headers(client_id="prepac-local-client", product="PrepaC"):
    return {
        "Accept": "application/json",
        "X-Plex-Client-Identifier": client_id,
        "X-Plex-Product": product,
        "X-Plex-Device-Name": product,
    }

def create_pin(client_id, product):
    r = requests.post(f"{PLEX_TV}/api/v2/pins", headers=plex_headers(client_id, product), params={"strong": "true"}, timeout=30, allow_redirects=False, stream=True)
    try:
        r.raise_for_status()
        data = _decode_json_response(r, MAX_PLEX_PIN_BYTES)
    finally:
        _close_response(r)
    save_pin(data["id"], data["code"], client_id, datetime.now().isoformat(timespec="seconds"), "pending")
    return {"id": data["id"], "code": data["code"]}

def check_pin(pin_id, client_id, product):
    r = requests.get(f"{PLEX_TV}/api/v2/pins/{pin_id}", headers=plex_headers(client_id, product), timeout=30, allow_redirects=False, stream=True)
    try:
        r.raise_for_status()
        data = _decode_json_response(r, MAX_PLEX_PIN_BYTES)
    finally:
        _close_response(r)
    token = data.get("authToken")
    if token:
        update_pin_status(pin_id, "authorized")
        return {"authorized": True, "token": token}
    return {"authorized": False}

def list_servers_for_token(token, client_id="prepac-local-client", product="PrepaC"):
    headers = plex_headers(client_id, product)
    headers["X-Plex-Token"] = token
    r = requests.get(f"{PLEX_TV}/api/resources", headers=headers, params={"includeHttps": "1"}, timeout=30, allow_redirects=False, stream=True)
    try:
        r.raise_for_status()
        raw = _bounded_response_bytes(r)
        encoding = _response_encoding(r)
    finally:
        _close_response(r)
    if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise RuntimeError("Plex response contained prohibited XML declarations")
    text = raw.decode(encoding, errors="replace")
    root = ET.fromstring(text)
    servers = []
    for dev in root.findall(".//Device"):
        provides = (dev.attrib.get("provides") or "")
        if "server" not in provides:
            continue
        connections = []
        for conn in dev.findall("./Connection"):
            uri = conn.attrib.get("uri")
            local = conn.attrib.get("local")
            address = conn.attrib.get("address")
            port = conn.attrib.get("port")
            if uri:
                try:
                    uri = normalize_plex_connection_uri(uri)
                except ValueError:
                    continue
                connections.append({
                    "uri": uri,
                    "local": local,
                    "address": address,
                    "port": port,
                })
        servers.append({
            "name": dev.attrib.get("name") or dev.attrib.get("clientIdentifier") or "Unknown",
            "clientIdentifier": dev.attrib.get("clientIdentifier"),
            "owned": dev.attrib.get("owned"),
            "connections": connections,
        })
    return servers

def save_selected_server(server_url, token=None):
    s = load_settings()
    patch = {
        "plex_url": normalize_plex_connection_uri(server_url) if str(server_url or "").strip() else ""
    }
    if token:
        patch["plex_token"] = token
    save_settings_patch(patch)
    s.update(patch)
    return s


def build_auth_url(client_id, product, code, forward_url):
    qs = urlencode({
        "clientID": client_id,
        "code": code,
        "forwardUrl": forward_url,
        "context[device][product]": product,
    })
    return f"{APP_PLEX}/auth#?{qs}"

def choose_best_server_connection(servers):
    secure_candidates = []
    fallback_candidates = []
    for server in servers or []:
        for conn in server.get("connections", []) or []:
            try:
                uri = normalize_plex_connection_uri(conn.get("uri"))
            except ValueError:
                continue
            if _is_loopback_connection(uri):
                continue
            local = str(conn.get("local") or "") == "1"
            score = 100 if local else 0
            target = secure_candidates if uri.startswith("https://") else fallback_candidates
            target.append((score, uri))
    candidates = secure_candidates or fallback_candidates
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]
