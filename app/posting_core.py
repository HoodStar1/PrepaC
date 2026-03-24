import json
import math
import os
import random
import re
import shutil
import shlex
import subprocess
import time
import pty
import select
import datetime
from collections import deque
from pathlib import Path

from app.secret_utils import resolve_secret
from app.packing_jobs import list_packing_history
from app.path_guardrails import assert_no_parent_traversal, assert_path_within_roots, build_allowed_roots
from app.posting_jobs import (
    add_posting_event,
    create_posting_job,
    finish_posting,
    get_running_provider_names,
    has_successful_posting,
    start_posting,
    update_posting_job,
    get_existing_active_posting_job_id,
    has_outdated_or_missing_successful_posting,
)

GB = 1024 ** 3

SECTION_RE = re.compile(r"^//([^/]+)//\s*$")
HEADER_RE = re.compile(r"^Header:\s*(.+?)\s*$", re.I | re.M)
GROUP_RE = re.compile(r"^Group:\s*(.+?)\s*$", re.I | re.M)
PASSWORD_RE = re.compile(r"^\[CODE\](.*?)\[/CODE\]\s*$", re.I | re.M | re.S)


def parse_randomizer_file(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    sections = {}
    current = None
    for raw in text.splitlines():
        m = SECTION_RE.match(raw.strip())
        if m:
            current = m.group(1).strip().lower()
            sections[current] = []
            continue
        if current and raw.strip():
            sections[current].append(raw.strip())
    return sections


def choose_from_and_groups(settings):
    rand_path = Path(settings.get("posting_randomizer_file") or "/app/NameRandomizer.txt")
    sections = parse_randomizer_file(rand_path)

    def pick(name, fallback):
        vals = sections.get(name, [])
        return random.choice(vals) if vals else fallback

    def slug_name(value):
        value = (value or "").strip().lower()
        value = re.sub(r"[^a-z0-9]+", ".", value)
        value = re.sub(r"\.+", ".", value).strip(".")
        return value or "user"

    def normalize_domain(isp_value, domain_value):
        isp_value = slug_name(isp_value)
        domain_value = (domain_value or "").strip().lower().lstrip("@")
        domain_value = re.sub(r"[^a-z0-9.\-]+", "", domain_value)
        domain_value = re.sub(r"\.{2,}", ".", domain_value).strip(".")

        # Required scheme: first.last@isp.domain
        # If domain is just a TLD like "ru", build "isp.ru"
        if not domain_value:
            return f"{isp_value}.com"
        if "." not in domain_value:
            return f"{isp_value}.{domain_value}"
        # If the randomizer already gave a full domain including the isp, keep it.
        if domain_value.startswith(isp_value + "."):
            return domain_value
        # Otherwise prepend isp to keep the required naming scheme.
        return f"{isp_value}.{domain_value}"

    first = pick("firstname", "John").strip()
    last = pick("lastname", "Doe").strip()
    isp = pick("ispname", "mail")
    domain = pick("domain", "example.com")

    email_domain = normalize_domain(isp, domain)
    mailbox = f"{slug_name(first)}.{slug_name(last)}@{email_domain}"
    from_header = f"{first} {last} <{mailbox}>"

    raw_groups = [g.strip() for g in sections.get("group", []) if g.strip()]
    groups_all = []
    for g in raw_groups:
        if g.lower().startswith("alt.binaries."):
            groups_all.append(g)
        else:
            groups_all.append(f"alt.binaries.{g}")
    groups_all = list(dict.fromkeys(groups_all))
    defaults = ["alt.binaries.boneless", "alt.binaries.multimedia", "alt.binaries.teevee"]
    if len(groups_all) >= 3:
        groups = random.sample(groups_all, 3)
    else:
        groups = list(dict.fromkeys(groups_all + defaults))[:3]
    return from_header, groups


def parse_template_info(template_path: Path):
    text = template_path.read_text(encoding="utf-8", errors="replace")
    header = HEADER_RE.search(text)
    group = GROUP_RE.search(text)
    password = PASSWORD_RE.search(text)
    return {
        "text": text,
        "header": header.group(1).strip() if header else "",
        "groups": group.group(1).strip() if group else "",
        "password": password.group(1).strip() if password else "",
    }


def update_template_groups(template_text: str, groups_csv: str):
    if GROUP_RE.search(template_text):
        return GROUP_RE.sub(f"Group: {groups_csv}", template_text, count=1)
    header_m = HEADER_RE.search(template_text)
    if header_m:
        insert_at = header_m.end()
        return template_text[:insert_at] + "\nGroup: " + groups_csv + template_text[insert_at:]
    return "Group: " + groups_csv + "\n" + template_text


def scan_posting_candidates(settings):
    packed_root = Path(settings.get("packing_output_root") or "/media/dest/_packed")
    output_root = packed_root / "output files"
    results = []
    if not packed_root.exists():
        return results

    packing_history = list_packing_history(5000)
    latest_packing_finished = {}
    for job in packing_history:
        if str(job.get("status", "")).lower() != "done":
            continue
        packed_path = str(job.get("output_root", "") or "").strip()
        finished_at = str(job.get("finished_at") or job.get("created_at") or "")
        if packed_path and (packed_path not in latest_packing_finished or finished_at > latest_packing_finished[packed_path]):
            latest_packing_finished[packed_path] = finished_at

    for item in sorted(packed_root.iterdir()):
        if not item.is_dir() or item.name == "output files":
            continue
        template = output_root / item.name / "template.txt"
        if not template.exists():
            continue
        packed_path = str(item)
        if get_existing_active_posting_job_id(packed_path):
            continue
        if not has_outdated_or_missing_successful_posting(packed_path, latest_packing_finished.get(packed_path, "")):
            continue
        size_bytes = sum(p.stat().st_size for p in item.rglob('*') if p.is_file())
        info = parse_template_info(template)
        results.append({
            "job_name": item.name,
            "packed_root": packed_path,
            "output_files_root": str(output_root / item.name),
            "template_path": str(template),
            "size_bytes": size_bytes,
            "header": info.get("header", ""),
            "password_present": bool(info.get("password")),
        })
    return results
    for item in sorted(packed_root.iterdir()):
        if not item.is_dir() or item.name == "output files":
            continue
        template = output_root / item.name / "template.txt"
        if not template.exists():
            continue
        if has_successful_posting(item.name):
            continue
        size_bytes = sum(p.stat().st_size for p in item.rglob('*') if p.is_file())
        info = parse_template_info(template)
        results.append({
            "job_name": item.name,
            "packed_root": str(item),
            "output_files_root": str(output_root / item.name),
            "template_path": str(template),
            "size_bytes": size_bytes,
            "header": info.get("header", ""),
            "password_present": bool(info.get("password")),
        })
    return results


def provider_config(settings, idx):
    prefix = f"posting_provider{idx}_"
    return {
        "name": f"provider{idx}",
        "enabled": str(settings.get(prefix + "enabled", "false")).lower() == "true",
        "host": settings.get(prefix + "host", "").strip(),
        "port": settings.get(prefix + "port", "563").strip(),
        "ssl": str(settings.get(prefix + "ssl", "true")).lower() == "true",
        "username": settings.get(prefix + "username", "").strip(),
        "password": resolve_secret(prefix + "password", settings),
        "connections": settings.get(prefix + "connections", "50").strip(),
    }




def redact_text_secret(value):
    if value is None:
        return None
    value = str(value)
    return "***REDACTED***" if value else value

def redact_cli_command(cmd):
    redacted = []
    secret_next = {"--password", "--user"}
    skip_next = False
    for i, part in enumerate(cmd):
        part = str(part)
        if skip_next:
            redacted.append("***REDACTED***")
            skip_next = False
            continue
        if part in secret_next:
            redacted.append(part)
            skip_next = True
            continue
        if part == "--comment":
            redacted.append(part)
            skip_next = True
            continue
        if part == "--nzb-password":
            redacted.append(part)
            skip_next = True
            continue
        redacted.append(part)
    return redacted

def shell_join(cmd):
    return " ".join(shlex.quote(str(x)) for x in cmd)

def validate_posting_inputs(settings, provider, packed_root: Path, output_files_root: Path, template_path: Path, header: str, password: str, from_header: str, groups):
    errors = []

    if not provider.get("enabled"):
        errors.append("Selected provider is not enabled.")
    if not provider.get("host"):
        errors.append("Provider host is required.")
    try:
        port = int(str(provider.get("port", "")).strip())
        if port <= 0 or port > 65535:
            errors.append("Provider port must be between 1 and 65535.")
    except Exception:
        errors.append("Provider port must be a valid integer.")
    try:
        conns = int(str(provider.get("connections", "")).strip())
        if conns <= 0:
            errors.append("Provider connections must be greater than 0.")
    except Exception:
        errors.append("Provider connections must be a valid integer.")

    # Do not apply a second hidden max-connections cap here.
    # Safe mode uses the configured provider connection count as the intended maximum
    # and reserves 1 connection for post-check when enabled.

    if not provider.get("username"):
        errors.append("Provider username is required.")
    if not provider.get("password"):
        errors.append("Provider password is required.")

    if not packed_root.exists() or not packed_root.is_dir():
        errors.append(f"Packed root does not exist: {packed_root}")
    else:
        file_count = sum(1 for p in packed_root.rglob("*") if p.is_file())
        if file_count == 0:
            errors.append(f"No files found to post in packed root: {packed_root}")

    if not template_path.exists():
        errors.append(f"Template file does not exist: {template_path}")
    if not output_files_root.exists():
        errors.append(f"Output files root does not exist: {output_files_root}")

    if not header.strip():
        errors.append("Header value is empty.")
    if not from_header.strip():
        errors.append("From header is empty.")
    elif not re.match(r"^[^<>]+ <[a-z0-9._%+\-]+@[a-z0-9.\-]+>$", from_header.strip(), re.I):
        errors.append(f"From header format looks invalid: {from_header}")

    if not groups:
        errors.append("No groups were generated.")
    else:
        bad_groups = [g for g in groups if not g.lower().startswith("alt.binaries.")]
        if bad_groups:
            errors.append("Generated groups contain invalid entries: " + ", ".join(bad_groups))

    try:
        art_size = int(str(settings.get("posting_article_size", "768000")).strip())
        if art_size <= 0:
            errors.append("Article size must be greater than 0.")
    except Exception:
        errors.append("Article size must be a valid integer.")

    try:
        line_size = int(str(settings.get("posting_yenc_line_size", "8000")).strip())
        if line_size <= 0:
            errors.append("yEnc line size must be greater than 0.")
    except Exception:
        errors.append("yEnc line size must be a valid integer.")

    try:
        retries = int(str(settings.get("posting_retries", "1")).strip())
        if retries < 0:
            errors.append("Posting retries cannot be negative.")
    except Exception:
        errors.append("Posting retries must be a valid integer.")

    return errors




def post_check_enabled(settings):
    return str(settings.get("posting_post_check", "false")).lower() == "true"

def effective_connections(settings, provider: dict):
    # The provider connection count in Settings is the user's intended max.
    # Reserve 1 connection for post-check when enabled, without applying a second hidden cap.
    requested = int(str(provider.get("connections", "1") or "1"))
    requested = max(1, requested)
    reserve = 1 if post_check_enabled(settings) else 0
    effective = max(1, requested - reserve)
    return effective, requested, reserve
def wait_for_provider(settings, size_bytes, job_id):
    provider1 = provider_config(settings, 1)
    provider2 = provider_config(settings, 2)
    size_limit_gb = float(settings.get("posting_provider2_max_gb_when_busy", "25") or 25)
    while True:
        running = set(get_running_provider_names())
        if provider1["enabled"] and provider1["name"] not in running:
            return provider1
        if (
            provider2["enabled"]
            and provider2["name"] not in running
            and size_bytes < size_limit_gb * GB
        ):
            return provider2
        update_posting_job(job_id, status="queued")
        add_posting_event(job_id, "queued", "Waiting for an available posting provider", None)
        time.sleep(5)


def build_nyuu_command(job_name, packed_root: Path, nzb_path: Path, header: str, password: str, groups_csv: str, from_header: str, provider: dict, settings):
    comment = settings.get("posting_comment", "").strip()
    subject = f"{header} [{{0filenum}}/{{files}}] - \"{{filename}}\" yEnc ({{part}}/{{parts}}) {{filesize}}"
    eff_connections, max_allowed, reserve = effective_connections(settings, provider)
    cmd = [
        "nyuu",
        "--host", provider["host"],
        "--port", str(provider["port"]),
        "--connections", str(eff_connections),
        "--from", from_header,
        "--groups", groups_csv.replace(", ", ","),
        "--subject", subject,
        "--article-size", str(settings.get("posting_article_size", "768000")),
        "--article-line-size", str(settings.get("posting_yenc_line_size", "8000")),
        "--post-retries", str(settings.get("posting_retries", "1")),
        "--post-retry-delay", str(settings.get("posting_retry_delay", "0s")),
        "-o", str(nzb_path),
        "--overwrite",
        "--minify",
        "--nzb-title", job_name,
        "--progress", "log:1s",
        "-T",
    ]
    if provider.get("ssl"):
        cmd.append("--ssl")
    if provider.get("username"):
        cmd += ["--user", provider["username"]]
    if provider.get("password"):
        cmd += ["--password", provider["password"]]
    if comment:
        cmd += ["--comment", comment]
    if str(settings.get("posting_post_check", "false")).lower() == "true":
        # Nyuu enables post checking via check-connections, not a bare --check flag.
        # Keep it conservative with one check connection and explicit retry knobs.
        cmd += [
            "--check-connections=1",
            "--check-tries", "2",
            "--check-delay", "5s",
            "--check-retry-delay", "30s",
            "--check-post-tries", "0",
        ]
    if str(settings.get("posting_embed_password_in_nzb", "true")).lower() == "true" and password:
        cmd += ["--nzb-password", password]
    cmd.append(str(packed_root))
    return cmd


def compress_nzb_with_rar(nzb_path: Path, output_rar: Path):
    output_rar.parent.mkdir(parents=True, exist_ok=True)
    if output_rar.exists():
        output_rar.unlink()
    subprocess.run(["rar", "a", "-ep1", str(output_rar), str(nzb_path)], check=True, cwd=str(nzb_path.parent))


def read_log_tail(path: Path, max_lines: int = 60):
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-max_lines:])
    except Exception:
        return "(unable to read posting log)"



def _to_gb(value, unit):
    try:
        v = float(value)
    except Exception:
        return 0.0
    unit = (unit or "").lower()
    # UI label says GB, but Nyuu reports KiB/MiB/GiB.
    # Convert to GiB-scale values for stable human-readable progress.
    if unit.startswith("gi"):
        return v
    if unit.startswith("mi"):
        return v / 1024.0
    if unit.startswith("ki"):
        return v / (1024.0 * 1024.0)
    return v

UPLOAD_RE = re.compile(r"Uploading\s+(\d+)\s+article\(s\)\s+from\s+(\d+)\s+file\(s\)\s+totalling\s+([0-9.]+)\s+([KMG]iB)", re.I)
READING_RE = re.compile(r"Reading file\s+(.+?)\.\.\.", re.I)
POSTED_RE = re.compile(r"Posted\s+(\d+)\s+article\(s\).*?\(([0-9.]+)\s+KiB/s\).*?(?:Network upload rate:\s*([0-9.]+)\s+KiB/s)?", re.I)
PROGRESS_LINE_RE = re.compile(r"(?P<pct>\d+(?:\.\d+)?)%.*?(?P<rate>[0-9.]+)\s*(?P<rate_unit>[KMG]iB/s).*?(?:ETA[: ]+)(?P<eta>[0-9dhms:]+)", re.I)
ARTICLE_PROGRESS_RE = re.compile(r"\[(?P<ts>.*?)\]\[INFO\]\s+Article posting progress:\s+(?P<read>\d+)\s+read,\s+(?P<posted>\d+)\s+posted(?:,\s+(?P<checked>\d+)\s+checked)?", re.I)
WARN_RE = re.compile(r"\[(?P<ts>.*?)\]\[(?:WARN|ERR )\]\s+(?P<msg>.*)", re.I)

ACTIVE_POSTING_OUTPUT = {}
ACTIVE_POSTING_STATS = {}

def _append_live_output(job_id, line):
    dq = ACTIVE_POSTING_OUTPUT.setdefault(int(job_id), deque(maxlen=400))
    dq.append(line)

def _set_live_stats(job_id, stats):
    ACTIVE_POSTING_STATS[int(job_id)] = {
        "transfer_rate": stats.get("transfer_rate",""),
        "percent_transferred": stats.get("percent_transferred",""),
        "eta": stats.get("eta",""),
    }

def get_posting_live_output(job_id):
    return "\n".join(ACTIVE_POSTING_OUTPUT.get(int(job_id), []))

def get_posting_live_stats(job_id):
    return ACTIVE_POSTING_STATS.get(int(job_id), {"transfer_rate":"","percent_transferred":"","eta":""})

def clear_posting_live(job_id):
    ACTIVE_POSTING_OUTPUT.pop(int(job_id), None)
    ACTIVE_POSTING_STATS.pop(int(job_id), None)

def stream_nyuu_process(cmd, cwd, posting_log: Path, job_id):
    state = {
        "total_articles": 0,
        "total_files": 0,
        "total_gb": 0.0,
        "last_progress_ts": None,
        "last_progress_posted": None,
        "last_checked": None,
        "last_warn": "",
        "postcheck_enabled": False,
    }

    master_fd, slave_fd = pty.openpty()
    env = dict(os.environ)
    env.setdefault("TERM", "xterm")

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=slave_fd,
        stderr=slave_fd,
        text=False,
        cwd=str(cwd),
        env=env,
        close_fds=True
    )
    os.close(slave_fd)

    pending = ""
    with open(posting_log, "a", encoding="utf-8", errors="replace") as logf:
        while True:
            ready, _, _ = select.select([master_fd], [], [], 1.0)
            if ready:
                try:
                    chunk = os.read(master_fd, 4096)
                except OSError:
                    chunk = b""
                if chunk:
                    pending += chunk.decode("utf-8", errors="replace")
                    while "\n" in pending:
                        line, pending = pending.split("\n", 1)
                        line = line.rstrip("\r")
                        logf.write(line + "\n")
                        logf.flush()
                        _append_live_output(job_id, line)
                        clean_line = re.sub(r"\x1b\[[0-9;]*m", "", line)

                        m = UPLOAD_RE.search(clean_line)
                        if m:
                            state["total_articles"] = int(m.group(1))
                            state["total_files"] = int(m.group(2))
                            state["total_gb"] = _to_gb(m.group(3), m.group(4))
                            msg = f"Transfer: 0.0% of {max(state['total_gb'], 0.01):.2f} GB, rate calculating..., ETA calculating..."
                            add_posting_event(job_id, "posting", msg, 22)
                            _set_live_stats(job_id, {"transfer_rate":"calculating...","percent_transferred":f"0.0% of {max(state['total_gb'], 0.01):.2f} GB","eta":"calculating..."})
                            continue

                        m = WARN_RE.search(clean_line)
                        if m:
                            warn_msg = m.group("msg").strip()
                            state["last_warn"] = warn_msg
                            if "retry post request" in warn_msg.lower():
                                add_posting_event(job_id, "finalizing", "Retrying final article after timeout", 96)
                            elif "disconnect timed out" in warn_msg.lower():
                                add_posting_event(job_id, "finalizing", "Recovering connection while finalizing upload", 96)
                            else:
                                add_posting_event(job_id, "finalizing", warn_msg, 96)
                            continue

                        m = ARTICLE_PROGRESS_RE.search(clean_line)
                        if m:
                            posted_now = int(m.group("posted"))
                            checked_now = m.group("checked")
                            checked_now = int(checked_now) if checked_now is not None else None
                            ts_str = m.group("ts")
                            if checked_now is not None:
                                state["postcheck_enabled"] = True

                            pct_value = (posted_now / max(1, state["total_articles"])) * 100.0 if state["total_articles"] else 0.0
                            rate_mib = 0.0
                            eta_value = "calculating..."

                            try:
                                current_ts = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                                if state["last_progress_ts"] is not None and state["last_progress_posted"] is not None:
                                    seconds = max(0.001, (current_ts - state["last_progress_ts"]).total_seconds())
                                    article_rate = max(0.0, (posted_now - state["last_progress_posted"]) / seconds)
                                    if state["total_articles"] > 0 and state["total_gb"] > 0:
                                        mib_total = state["total_gb"] * 1024.0
                                        mib_per_article = mib_total / state["total_articles"]
                                        rate_mib = article_rate * mib_per_article
                                        remaining_articles = max(0, state["total_articles"] - posted_now)
                                        if article_rate > 0 and remaining_articles > 0:
                                            eta_seconds = int(round(remaining_articles / article_rate))
                                            hrs = eta_seconds // 3600
                                            mins = (eta_seconds % 3600) // 60
                                            secs = eta_seconds % 60
                                            eta_value = f"{hrs}:{mins:02d}:{secs:02d}" if hrs > 0 else f"{mins}:{secs:02d}"
                                state["last_progress_ts"] = current_ts
                                state["last_progress_posted"] = posted_now
                            except Exception:
                                pass

                            percent_text = f"{pct_value:.1f}% of {max(state['total_gb'], 0.01):.2f} GB"

                            # decide phase after 100%
                            if checked_now is not None and posted_now >= state["total_articles"]:
                                state["last_checked"] = checked_now
                                if checked_now < state["total_articles"]:
                                    verify_pct = (checked_now / max(1, state["total_articles"])) * 100.0
                                    add_posting_event(
                                        job_id,
                                        "postcheck",
                                        f"Post-check verification: {checked_now}/{state['total_articles']} checked",
                                        96 + int(min(3, verify_pct / 34.0))
                                    )
                                else:
                                    add_posting_event(job_id, "postcheck", "Post-check verification complete, finalizing upload", 99)
                            elif checked_now is None and posted_now >= state["total_articles"]:
                                add_posting_event(job_id, "finalizing", "Upload sent, waiting for Nyuu to finalize and exit", 96)
                            elif checked_now is None and posted_now >= max(0, state["total_articles"] - 1):
                                if state["last_warn"]:
                                    add_posting_event(job_id, "finalizing", state["last_warn"], 96)
                                else:
                                    add_posting_event(job_id, "finalizing", "Finishing last article and waiting for Nyuu to exit", 96)
                            else:
                                msg = f"Transfer: {percent_text}, rate {(f'{rate_mib:.2f} MiB/s' if rate_mib > 0 else 'calculating...')}, ETA {eta_value if eta_value != '--' else 'calculating...'}"
                                add_posting_event(job_id, "posting", msg, min(95, 20 + int(pct_value * 0.75)))

                            _set_live_stats(
                                job_id,
                                {
                                    "transfer_rate": (f"{rate_mib:.2f} MiB/s" if rate_mib > 0 else "calculating..."),
                                    "percent_transferred": percent_text,
                                    "eta": (eta_value if eta_value != "--" else "calculating..."),
                                },
                            )
                            continue
            if proc.poll() is not None:
                if pending:
                    for line in pending.splitlines():
                        _append_live_output(job_id, line)
                        logf.write(line + "\n")
                    logf.flush()
                break
    try:
        os.close(master_fd)
    except Exception:
        pass
    return proc.wait(), state
def run_posting_job(job_id, packed_root_str, output_files_root_str, template_path_str, settings):
    allowed_roots = build_allowed_roots(settings)
    packed_root = Path(packed_root_str)
    output_files_root = Path(output_files_root_str)
    template_path = Path(template_path_str)
    job_name = packed_root.name
    posted_root = Path(settings.get("posting_posted_root") or "/media/dest/_posted") / job_name
    nzb_tmp_root = posted_root
    nzb_rar_root = Path(settings.get("posting_nzb_root") or "/media/dest/_nzb")
    size_bytes = sum(p.stat().st_size for p in packed_root.rglob('*') if p.is_file()) if packed_root.exists() else 0
    posting_log = posted_root / "posting.log"

    try:
        provider = wait_for_provider(settings, size_bytes, job_id)
        start_posting(job_id, provider_used=provider["name"])
        add_posting_event(job_id, "prepare", f"Using {provider['name']} for posting", 5)

        info = parse_template_info(template_path)
        header = info.get("header") or job_name
        password = info.get("password", "")
        from_header, groups = choose_from_and_groups(settings)
        groups_csv = ", ".join(groups)
        update_posting_job(
            job_id,
            from_header=from_header,
            groups_csv=groups_csv,
            header_value=header,
            password_value=password,
            size_bytes=size_bytes,
        )

        assert_path_within_roots(posted_root, allowed_roots, "posted root")
        assert_path_within_roots(nzb_tmp_root, allowed_roots, "nzb tmp root")
        assert_path_within_roots(nzb_rar_root, allowed_roots, "nzb rar root")
        posted_root.mkdir(parents=True, exist_ok=True)
        nzb_path = nzb_tmp_root / f"{job_name}.nzb"
        eff_connections, max_allowed, reserve = effective_connections(settings, provider)
        cmd = build_nyuu_command(job_name, packed_root, nzb_path, header, password, groups_csv, from_header, provider, settings)

        errors = validate_posting_inputs(
            settings, provider, packed_root, output_files_root, template_path,
            header, password, from_header, groups
        )

        with open(posting_log, "w", encoding="utf-8", errors="replace") as logf:
            logf.write("=== NYUU SAFE MODE VALIDATION ===\n")
            logf.write(f"Job: {job_name}\n")
            logf.write(f"Provider: {provider.get('name')}\n")
            logf.write(f"Packed root: {packed_root}\n")
            logf.write(f"Template: {template_path}\n")
            logf.write(f"Output files root: {output_files_root}\n")
            logf.write(f"Posted root: {posted_root}\n")
            logf.write(f"NZB path: {nzb_path}\n")
            logf.write(f"Header: {header}\n")
            logf.write(f"From: {from_header}\n")
            logf.write(f"Groups: {groups_csv}\n")
            logf.write(f"Command: {shell_join(redact_cli_command(cmd))}\n\n")
            if errors:
                logf.write("Validation errors:\n")
                for e in errors:
                    logf.write(f"- {e}\n")

        non_connection_errors = [e for e in errors if "connections" not in e.lower()]
        if non_connection_errors:
            raise RuntimeError("Posting validation failed: " + " | ".join(non_connection_errors))

        update_posting_job(job_id, nzb_path=str(nzb_path), phase="posting")
        add_posting_event(job_id, "posting", "Starting Nyuu posting", 20)

        with open(posting_log, "a", encoding="utf-8", errors="replace") as logf:
            logf.write("\n=== NYUU OUTPUT ===\n")
            logf.flush()

        rc, _state = stream_nyuu_process(cmd, packed_root, posting_log, job_id)

        with open(posting_log, "a", encoding="utf-8", errors="replace") as logf:
            logf.write(f"\nExit code: {rc}\n")

        if rc != 0:
            raise RuntimeError(f"Nyuu exited with code {rc}")

        add_posting_event(job_id, "postcheck", "Upload finished, waiting for Nyuu post-check and finalization", 96)

        add_posting_event(job_id, "template", "Updating template.txt with posted groups", 97)
        updated_template = update_template_groups(info["text"], groups_csv)
        template_path.write_text(updated_template, encoding="utf-8", errors="replace")

        add_posting_event(job_id, "nzb", "Compressing NZB with RAR", 98)
        nzb_rar_path = nzb_rar_root / f"{job_name}.rar"
        compress_nzb_with_rar(nzb_path, nzb_rar_path)

        add_posting_event(job_id, "finalize", "Moving output files to posted folder", 99)
        if output_files_root.exists():
            for child in sorted(output_files_root.iterdir()):
                target = posted_root / child.name
                if target.exists():
                    if target.is_dir():
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                shutil.move(str(child), str(target))
            try:
                output_files_root.rmdir()
            except Exception:
                pass

        if nzb_path.exists():
            nzb_path.unlink()

        update_posting_job(
            job_id,
            nzb_rar_path=str(nzb_rar_path),
            posted_root=str(posted_root),
            template_path=str(posted_root / "template.txt"),
            nzb_path=str(nzb_rar_path)
        )

        add_posting_event(job_id, "cleanup", "Deleting packed source after successful posting", 99)
        if packed_root.exists():
            shutil.rmtree(packed_root)

        finish_posting(job_id, True, "Posting complete")
        add_posting_event(job_id, "complete", "Posting complete", 100)
        # keep live output briefly available; stats remain for UI/history
    except Exception as e:
        try:
            assert_path_within_roots(posted_root, allowed_roots, "posted root")
            posted_root.mkdir(parents=True, exist_ok=True)
            with open(posting_log, "a", encoding="utf-8", errors="replace") as logf:
                logf.write("\n=== FAILURE ===\n")
                logf.write(str(e) + "\n")
        except Exception:
            pass
        finish_posting(job_id, False, str(e))
        add_posting_event(job_id, "failed", str(e), None)
def start_posting_job_async(packed_root, settings):
    packed_root = Path(packed_root)
    output_files_root = Path(settings.get("packing_output_root") or "/media/dest/_packed") / "output files" / packed_root.name
    template_path = output_files_root / "template.txt"
    size_bytes = sum(p.stat().st_size for p in packed_root.rglob('*') if p.is_file()) if packed_root.exists() else 0
    job_id = create_posting_job(packed_root.name, str(packed_root), str(output_files_root), str(template_path), size_bytes)
    import threading
    threading.Thread(target=run_posting_job, args=(job_id, str(packed_root), str(output_files_root), str(template_path), settings), daemon=True).start()
    return job_id
