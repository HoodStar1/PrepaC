import ipaddress
import re
import secrets
import socket
from urllib.parse import urlsplit, urlunsplit


UNSAFE_HTTP_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
DEFAULT_SHARE_IMPORT_MAX_MIB = 128
DEFAULT_TRUSTED_PROXY_NETWORKS = "127.0.0.1/32,::1/128"
_HOST_RE = re.compile(r"^[A-Za-z0-9._:\[\]-]+$")


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
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError("Destination base_url contains an invalid port") from exc
    if port is not None and not (1 <= int(port) <= 65535):
        raise ValueError("Destination base_url contains an invalid port")

    path = parts.path.rstrip("/")
    return urlunsplit((scheme, parts.netloc, path, "", ""))


def _first_forwarded_value(value):
    return str(value or "").split(",")[0].strip()


def parse_trusted_proxy_networks(value=None):
    raw = DEFAULT_TRUSTED_PROXY_NETWORKS if value is None else str(value or "")
    networks = []
    for item in raw.split(","):
        candidate = item.strip()
        if not candidate:
            continue
        try:
            networks.append(ipaddress.ip_network(candidate, strict=False))
        except ValueError:
            continue
    return tuple(networks)


def is_trusted_proxy_peer(peer_address, configured_networks=None):
    try:
        address = ipaddress.ip_address(str(peer_address or "").strip())
    except ValueError:
        return False
    networks = parse_trusted_proxy_networks(configured_networks)
    return any(address in network for network in networks)


def _parse_forwarded_address(value):
    candidate = str(value or "").strip().strip('"')
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1]
    if "%" in candidate:
        candidate = candidate.split("%", 1)[0]
    return ipaddress.ip_address(candidate)


def resolve_client_ip(peer_address, forwarded_for="", configured_networks=None):
    """Resolve a client address without trusting caller-controlled leftmost hops.

    X-Forwarded-For is ordered from the original client to the proxy nearest the
    application. Starting at the socket peer, walk right-to-left only while the
    current hop is trusted and stop at the first untrusted address. A malformed
    hop conservatively stops resolution at the last verified address.
    """
    try:
        current = _parse_forwarded_address(peer_address)
    except ValueError:
        return str(peer_address or "unknown").strip() or "unknown"

    networks = parse_trusted_proxy_networks(configured_networks)
    if not any(current in network for network in networks):
        return str(current)

    hops = [item.strip() for item in str(forwarded_for or "").split(",") if item.strip()]
    for raw_hop in reversed(hops):
        try:
            candidate = _parse_forwarded_address(raw_hop)
        except ValueError:
            break
        current = candidate
        if not any(current in network for network in networks):
            break
    return str(current)


def normalize_request_host(value):
    host = str(value or "").strip()
    if not host or len(host) > 255 or any(ord(ch) < 33 for ch in host):
        raise ValueError("Invalid request host")
    if any(ch in host for ch in "/\\@?#") or not _HOST_RE.fullmatch(host):
        raise ValueError("Invalid request host")
    parsed = urlsplit(f"//{host}")
    if not parsed.hostname:
        raise ValueError("Invalid request host")
    try:
        parsed.port
    except ValueError as exc:
        raise ValueError("Invalid request host port") from exc
    return host


def host_is_allowed(host, trusted_hosts=None):
    normalized = normalize_request_host(host)
    rules = [item.strip().lower() for item in str(trusted_hosts or "").split(",") if item.strip()]
    if not rules:
        return True
    hostname = str(urlsplit(f"//{normalized}").hostname or "").lower()
    for rule in rules:
        if rule.startswith("*.") and (hostname == rule[2:] or hostname.endswith(rule[1:])):
            return True
        if hostname == rule:
            return True
    return False


def validate_outbound_url(value, *, allow_private=True, allowed_ports=None, resolver=socket.getaddrinfo):
    """Normalize a service URL and reject unsafe resolved address classes."""
    normalized = normalize_service_base_url(value)
    parts = urlsplit(normalized)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    if allowed_ports and port not in {int(item) for item in allowed_ports}:
        raise ValueError("Destination port is not allowed")
    try:
        resolved = resolver(parts.hostname, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ValueError("Destination hostname could not be resolved") from exc
    addresses = set()
    for item in resolved:
        sockaddr = item[4]
        if sockaddr:
            addresses.add(str(sockaddr[0]).split("%")[0])
    if not addresses:
        raise ValueError("Destination hostname did not resolve to an address")
    for raw in addresses:
        address = ipaddress.ip_address(raw)
        if address.is_unspecified or address.is_multicast or address.is_link_local:
            raise ValueError("Destination resolves to a disallowed address range")
        if not allow_private and (address.is_private or address.is_loopback):
            raise ValueError("Private destination addresses are disabled")
    return normalized


def build_external_base_url(
    request_scheme,
    request_host,
    forwarded_proto="",
    forwarded_host="",
    trust_proxy=False,
    peer_address=None,
    trusted_proxy_networks=None,
    trusted_hosts=None,
):
    proto = str(request_scheme or "http").strip().lower() or "http"
    host = normalize_request_host(request_host)

    proxy_allowed = bool(trust_proxy)
    if proxy_allowed and peer_address is not None:
        proxy_allowed = is_trusted_proxy_peer(peer_address, trusted_proxy_networks)
    if proxy_allowed:
        candidate_proto = _first_forwarded_value(forwarded_proto).lower()
        if candidate_proto in {"http", "https"}:
            proto = candidate_proto
        candidate_host = _first_forwarded_value(forwarded_host)
        if candidate_host and host_is_allowed(candidate_host, trusted_hosts):
            host = candidate_host

    if not host_is_allowed(host, trusted_hosts):
        raise ValueError("Request host is not trusted")

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
