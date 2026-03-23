
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urlencode
import requests
from app.secret_utils import resolve_secret
from app.db import save_pin, update_pin_status, save_settings, load_settings

PLEX_TV = "https://plex.tv"
APP_PLEX = "https://app.plex.tv"

def plex_headers(client_id="prepac-local-client", product="PrepaC"):
    return {
        "Accept": "application/json",
        "X-Plex-Client-Identifier": client_id,
        "X-Plex-Product": product,
        "X-Plex-Device-Name": product,
    }

def create_pin(client_id, product):
    r = requests.post(f"{PLEX_TV}/api/v2/pins", headers=plex_headers(client_id, product), params={"strong": "true"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    save_pin(data["id"], data["code"], client_id, datetime.now().isoformat(timespec="seconds"), "pending")
    return {"id": data["id"], "code": data["code"]}

def check_pin(pin_id, client_id, product):
    r = requests.get(f"{PLEX_TV}/api/v2/pins/{pin_id}", headers=plex_headers(client_id, product), timeout=30)
    r.raise_for_status()
    data = r.json()
    token = data.get("authToken")
    if token:
        s = load_settings()
        s["plex_token"] = token
        save_settings(s)
        update_pin_status(pin_id, "authorized")
        return {"authorized": True, "token": token}
    return {"authorized": False}

def list_servers_for_token(token, client_id="prepac-local-client", product="PrepaC"):
    headers = plex_headers(client_id, product)
    headers["X-Plex-Token"] = token
    r = requests.get(f"{PLEX_TV}/api/resources", headers=headers, params={"includeHttps": "1"}, timeout=30)
    r.raise_for_status()
    text = r.text
    root = ET.fromstring(text)
    servers = []
    for dev in root.findall(".//Device"):
        provides = (dev.attrib.get("provides") or "")
        if "server" not in provides:
            continue
        access_token = dev.attrib.get("accessToken") or token
        connections = []
        for conn in dev.findall("./Connection"):
            uri = conn.attrib.get("uri")
            local = conn.attrib.get("local")
            address = conn.attrib.get("address")
            port = conn.attrib.get("port")
            if uri:
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
            "accessToken": access_token,
            "connections": connections,
        })
    return servers

def save_selected_server(server_url, token=None):
    s = load_settings()
    s["plex_url"] = (server_url or "").strip()
    if token:
        s["plex_token"] = token
    save_settings(s)
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
    candidates = []
    for server in servers or []:
        for conn in server.get("connections", []) or []:
            uri = (conn.get("uri") or "").strip()
            if not uri:
                continue
            local = str(conn.get("local") or "") == "1"
            https = uri.startswith("https://")
            score = 0
            if local:
                score += 100
            if not https:
                score += 20
            if "127.0.0.1" in uri or "localhost" in uri:
                score -= 1000
            candidates.append((score, uri))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]
