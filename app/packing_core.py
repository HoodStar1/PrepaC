import csv
import json
import math
import os
import random
import re
import shutil
import string
import subprocess
import time
from threading import Lock
import pathlib
from pathlib import Path
from urllib.parse import quote

import requests
from PIL import Image, ImageOps, ImageDraw, ImageFont

from app.media_probe import detect_tags, build_bracket_from_detected
from app.db import load_settings
from app.jobs import list_jobs
from app.secret_utils import resolve_secret
from app.path_guardrails import assert_no_parent_traversal, assert_path_within_roots, build_allowed_roots
from app.packing_jobs import (
    add_packing_event,
    create_packing_job,
    finish_packing,
    has_successful_packing,
    list_packing_jobs,
    start_packing,
    update_packing_job,
    get_existing_active_packing_job_id,
    has_outdated_or_missing_successful_packing,
    count_running_packing_jobs,
    try_claim_packing_slot,
    latest_successful_packing_job_id,
    reconcile_orphaned_running_packing_jobs,
)

VIDEO_EXTS = {".mkv",".mp4",".avi",".mov",".m4v",".ts",".m2ts",".wmv",".mpg",".mpeg"}
MB = 1024*1024
GB = 1024*1024*1024
ALLOWED_VOLUMES = [50*MB,100*MB,250*MB,500*MB,1*GB,2*GB]

SECTION_RE = re.compile(r"^//([^/]+)//\s*$")
PACKING_SLOT_LOCK = Lock()
PACKING_ACTIVE_COUNT = 0
PACKING_ACTIVE_JOB_IDS = set()


def _mark_packing_job_active(job_id):
    with PACKING_SLOT_LOCK:
        PACKING_ACTIVE_JOB_IDS.add(int(job_id))


def _mark_packing_job_inactive(job_id):
    with PACKING_SLOT_LOCK:
        PACKING_ACTIVE_JOB_IDS.discard(int(job_id))


def reconcile_orphaned_packing_jobs_in_process():
    with PACKING_SLOT_LOCK:
        active_ids = set(PACKING_ACTIVE_JOB_IDS)
    return reconcile_orphaned_running_packing_jobs(active_ids)

def is_video(p: Path):
    return p.suffix.lower() in VIDEO_EXTS

def folder_size(path: Path):
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except Exception:
            pass
    return total

def largest_video(path: Path):
    best = None; best_size = -1
    for p in path.rglob("*"):
        try:
            if p.is_file() and is_video(p):
                sz = p.stat().st_size
                if sz > best_size:
                    best_size = sz
                    best = p
        except Exception:
            pass
    return best

def scan_watch_folder(settings):
    reconcile_orphaned_packing_jobs_in_process()
    watch = Path(settings.get("packing_watch_root") or settings.get("dest_root") or "/media/dest")
    jobs = []
    if not watch.exists():
        return jobs

    prepare_jobs = list_jobs(5000)
    latest_prepare_finished = {}
    for job in prepare_jobs:
        if str(job.get("status", "")).lower() != "done":
            continue
        dest_path = str(job.get("dest_path", "") or "").strip()
        if not dest_path:
            continue
        finished_at = str(job.get("finished_at") or job.get("created_at") or "")
        if dest_path not in latest_prepare_finished or finished_at > latest_prepare_finished[dest_path]:
            latest_prepare_finished[dest_path] = finished_at

    for p in sorted([x for x in watch.iterdir() if x.is_dir()]):
        if p.name.startswith("_packed"):
            continue
        source_path = str(p)
        if get_existing_active_packing_job_id(source_path):
            continue
        if not has_outdated_or_missing_successful_packing(source_path, latest_prepare_finished.get(source_path, "")):
            continue
        size = folder_size(p)
        rep = largest_video(p)
        probe = detect_tags(str(rep)) if rep else {}
        chosen_bracket = build_bracket_from_detected(probe) if probe else ""
        jobs.append({
            "source_path": source_path,
            "job_name": p.name,
            "size_bytes": size,
            "size_gb": round(size / GB, 2),
            "largest_video": str(rep) if rep else "",
            "detected_tags": probe,
            "chosen_bracket": chosen_bracket,
            "estimated_volume": choose_volume_size(size)[0],
            "estimated_parts": choose_volume_size(size)[1],
            "estimated_par2_percent": choose_par2_percent(size),
        })
    return jobs
    for p in sorted([x for x in watch.iterdir() if x.is_dir()]):
        if p.name.startswith("_packed"):
            continue
        if has_successful_packing(str(p)):
            continue
        size = folder_size(p)
        rep = largest_video(p)
        probe = detect_tags(str(rep)) if rep else {}
        chosen_bracket = build_bracket_from_detected(probe) if probe else ""
        jobs.append({
            "source_path": str(p),
            "job_name": p.name,
            "size_bytes": size,
            "size_gb": round(size / GB, 2),
            "largest_video": str(rep) if rep else "",
            "detected_tags": probe,
            "chosen_bracket": chosen_bracket,
            "estimated_volume": choose_volume_size(size)[0],
            "estimated_parts": choose_volume_size(size)[1],
            "estimated_par2_percent": choose_par2_percent(size),
        })
    return jobs

def choose_volume_size(total_bytes):
    estimates = [(s, math.ceil(total_bytes / s) if s else 0) for s in ALLOWED_VOLUMES]
    valid = [e for e in estimates if 50 <= e[1] <= 500]
    if valid:
        chosen = min(valid, key=lambda x: abs(x[1]-175))
    elif all(e[1] < 50 for e in estimates):
        chosen = max(estimates, key=lambda x: x[1])
    else:
        chosen = min(estimates, key=lambda x: x[1])
    return chosen

def choose_par2_percent(total_bytes):
    if total_bytes < 10*GB: return 30
    if total_bytes < 25*GB: return 20
    if total_bytes < 50*GB: return 15
    if total_bytes < 100*GB: return 10
    return 7


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

def choose_random_groups(settings):
    rand_path = Path(settings.get("posting_randomizer_file") or "/app/NameRandomizer.txt")
    try:
        sections = parse_randomizer_file(rand_path)
        raw_groups = [g.strip() for g in sections.get("group", []) if g.strip()]
    except Exception:
        raw_groups = []
    groups_all = []
    for g in raw_groups:
        if g.lower().startswith("alt.binaries."):
            groups_all.append(g)
        else:
            groups_all.append(f"alt.binaries.{g}")
    groups_all = list(dict.fromkeys(groups_all))
    defaults = ["alt.binaries.boneless", "alt.binaries.multimedia", "alt.binaries.teevee"]
    if len(groups_all) >= 3:
        return ", ".join(random.sample(groups_all, 3))
    return ", ".join(list(dict.fromkeys(groups_all + defaults))[:3])

def generate_password(prefix, length):
    remaining = max(0, int(length) - len(prefix))
    alphabet = string.ascii_letters + string.digits
    return prefix + "".join(random.choices(alphabet, k=remaining))

def generate_archive_token(length=15, fixed_tag="FS", fixed_pos=4):
    length = int(length); fixed_pos = int(fixed_pos)
    chars = list("".join(random.choices(string.ascii_letters + string.digits, k=length)))
    start = max(1, fixed_pos) - 1
    if start + len(fixed_tag) <= length:
        chars[start:start+len(fixed_tag)] = list(fixed_tag)
    return "".join(chars[:length])

def detect_hdr_filter(media_info):
    raw = (media_info or {}).get("raw_mediainfo","").lower()
    if "hlg" in raw:
        return "zscale=t=linear:npl=100,tonemap=mobius,zscale=t=bt709:m=bt709:r=tv"
    if "hdr10" in raw or "dolby vision" in raw:
        return "zscale=t=linear:npl=100,tonemap=hable,zscale=t=bt709:m=bt709:r=tv"
    return ""

def ffprobe_duration(video_path):
    try:
        out = subprocess.check_output(["ffprobe","-v","error","-show_entries","format=duration","-of","default=nk=1:nw=1",str(video_path)], stderr=subprocess.STDOUT).decode().strip()
        return float(out)
    except Exception:
        return 0.0

def _format_timestamp(seconds_value):
    total = max(0, int(seconds_value or 0))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:01d}:{m:02d}:{s:02d}"

def _draw_timestamp(draw, x, y, text):
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
    except Exception:
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x = 8
    pad_y = 3
    pill = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2)
    draw.rounded_rectangle(pill, radius=8, fill=(232, 232, 232, 235), outline=(70, 70, 70, 255), width=1)
    draw.text((x + pad_x, y + pad_y - 1), text, font=font, fill=(20, 20, 20, 255))

def _format_timestamp(seconds_value):
    total = max(0, int(seconds_value or 0))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:01d}:{m:02d}:{s:02d}"

def _draw_timestamp(draw, x, y, text):
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
    except Exception:
        font = ImageFont.load_default()
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x = 8
    pad_y = 2
    pill = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2)
    draw.rounded_rectangle(pill, radius=8, fill=(232, 232, 232, 235), outline=(70, 70, 70, 255), width=1)
    draw.text((x + pad_x, y + pad_y - 1), text, font=font, fill=(20, 20, 20, 255))

def _draw_progress_bar(draw, x, y, width, ratio):
    ratio = max(0.0, min(1.0, float(ratio or 0.0)))
    h = 6
    draw.rounded_rectangle((x, y, x + width, y + h), radius=3, fill=(210, 210, 210, 230))
    draw.rounded_rectangle((x, y, x + int(width * ratio), y + h), radius=3, fill=(30, 135, 255, 255))

def _parse_mediainfo_sections(raw_text):
    sections = []
    current_name = None
    current_data = []
    for line in (raw_text or "").splitlines():
        stripped = line.strip()
        if stripped in {"General", "Video", "Audio", "Menu"} or stripped.startswith("Text"):
            if current_name:
                sections.append((current_name, current_data))
            current_name = stripped
            current_data = []
            continue
        if current_name:
            current_data.append(line.rstrip())
    if current_name:
        sections.append((current_name, current_data))
    return sections

def _section_to_map(lines):
    data = {}
    items = []
    for line in lines:
        if " : " in line:
            k, v = line.split(" : ", 1)
            k = k.strip()
            v = v.strip()
            data.setdefault(k, []).append(v)
            items.append((k, v))
    return data, items

def build_summary_and_fullscan(video_path):
    raw = mediainfo_summary(video_path)
    sections = _parse_mediainfo_sections(raw)
    general = {}
    video = {}
    audio = {}
    texts = []
    menu_items = []
    for name, lines in sections:
        m, items = _section_to_map(lines)
        if name == "General" and not general:
            general = m
        elif name == "Video" and not video:
            video = m
        elif name == "Audio" and not audio:
            audio = m
        elif name.startswith("Text"):
            texts.append(m)
        elif name == "Menu":
            menu_items.extend(items)

    def first(m, key, default=""):
        vals = m.get(key, [])
        return vals[0] if vals else default

    title = pathlib.Path(video_path).name
    fmt = first(general, "Format")
    size = first(general, "File size")
    duration = first(general, "Duration")
    overall = first(general, "Overall bit rate")
    v_fmt = first(video, "Commercial name") or first(video, "Format")
    v_bitrate = first(video, "Bit rate")
    v_height = first(video, "Height")
    v_fps = first(video, "Frame rate")
    v_ar = first(video, "Display aspect ratio")
    v_depth = first(video, "Bit depth")
    a_lang = first(audio, "Language")
    a_fmt = first(audio, "Commercial name") or first(audio, "Format/Info") or first(audio, "Format")
    a_ch = first(audio, "Channel(s)")
    a_sr = first(audio, "Sampling rate")
    a_br = first(audio, "Bit rate")

    summary = []
    summary.append(f"Title: {title}")
    if fmt: summary.append(f"Format: {fmt}")
    if size: summary.append(f"File size: {size}")
    if duration: summary.append(f"Duration: {duration}")
    if overall: summary.append(f"Overall bit rate: {overall}")
    video_parts = [p for p in [v_fmt, v_bitrate, v_height, v_fps, v_ar, v_depth] if p]
    if video_parts:
        if v_height and "pixels" not in v_height:
            video_parts[video_parts.index(v_height)] = f"{v_height} pixels"
        if v_fps and "FPS" not in v_fps:
            video_parts[video_parts.index(v_fps)] = f"{v_fps} FPS"
        if v_depth and "bits" not in v_depth:
            video_parts[video_parts.index(v_depth)] = f"{v_depth} bits"
        summary.append("Video: " + " / ".join(video_parts))
    audio_parts = [p for p in [a_lang, a_fmt, a_ch, a_sr, a_br] if p]
    if audio_parts:
        summary.append("Audio: " + " / ".join(audio_parts))
    for t in texts:
        lang = first(t, "Language")
        bitrate = first(t, "Bit rate")
        title2 = first(t, "Title")
        disp = lang
        if title2 and title2.lower() not in {"sdh"}:
            disp = f"{lang} ({title2})" if lang else title2
        elif title2 and title2.lower() == "sdh":
            disp = f"{lang} / {title2}" if lang else title2
        if disp and bitrate:
            summary.append(f"Subtitle: {disp} / {bitrate}")
        elif disp:
            summary.append(f"Subtitle: {disp}")
    if menu_items:
        summary.append(f"Chapters: {len(menu_items)}")
    summary_text = "\n".join(summary) + "\n\n"

    fullscan = []
    fullscan.append(f"Disc Label:     {title}")
    try:
        fullscan.append(f"Disc Size:      {pathlib.Path(video_path).stat().st_size:,} bytes")
    except Exception:
        fullscan.append("Disc Size:      0 bytes")
    fullscan.append("")
    fullscan.append(raw.rstrip())
    return summary_text, "\n".join(fullscan)

def _format_ts(seconds_value):
    total = max(0, int(seconds_value or 0))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def _load_font(size=14):
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]:
        try:
            return ImageFont.truetype(candidate, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _draw_time_badge(draw, x, y, text):
    font = _load_font(14)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x = 8
    pad_y = 3
    rect = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2)
    draw.rounded_rectangle(rect, radius=8, fill=(235, 235, 235, 225), outline=(80, 80, 80, 255), width=1)
    draw.text((x + pad_x, y + pad_y - 1), text, font=font, fill=(20, 20, 20, 255))

def _draw_progress_bar(draw, x, y, w, h, fraction):
    draw.rounded_rectangle((x, y, x + w, y + h), radius=max(1, h//2), fill=(245, 245, 245, 230))
    inner = max(0, min(w, int(w * max(0.0, min(1.0, fraction)))))
    if inner > 0:
        draw.rounded_rectangle((x, y, x + inner, y + h), radius=max(1, h//2), fill=(210, 210, 210, 255))

def _format_ts(seconds_value):
    total = max(0, int(seconds_value or 0))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def _load_font(size=14):
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]:
        try:
            return ImageFont.truetype(candidate, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _draw_time_badge(draw, x, y, text):
    font = _load_font(14)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x = 8
    pad_y = 3
    rect = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2)
    draw.rounded_rectangle(rect, radius=8, fill=(235, 235, 235, 225), outline=(80, 80, 80, 255), width=1)
    draw.text((x + pad_x, y + pad_y - 1), text, font=font, fill=(20, 20, 20, 255))

def _draw_progress_bar(draw, x, y, w, h, fraction):
    draw.rounded_rectangle((x, y, x + w, y + h), radius=max(1, h//2), fill=(245, 245, 245, 230))
    inner = max(0, min(w, int(w * max(0.0, min(1.0, fraction)))))
    if inner > 0:
        draw.rounded_rectangle((x, y, x + inner, y + h), radius=max(1, h//2), fill=(210, 210, 210, 255))

def _format_ts(seconds_value):
    total = max(0, int(seconds_value or 0))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def _load_font(size=14):
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]:
        try:
            return ImageFont.truetype(candidate, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _draw_time_badge(draw, x, y, text):
    font = _load_font(14)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x = 8
    pad_y = 3
    rect = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2)
    draw.rounded_rectangle(rect, radius=8, fill=(235, 235, 235, 225), outline=(80, 80, 80, 255), width=1)
    draw.text((x + pad_x, y + pad_y - 1), text, font=font, fill=(20, 20, 20, 255))

def _draw_progress_bar(draw, x, y, w, h, fraction):
    draw.rounded_rectangle((x, y, x + w, y + h), radius=max(1, h//2), fill=(245, 245, 245, 230))
    inner = max(0, min(w, int(w * max(0.0, min(1.0, fraction)))))
    if inner > 0:
        draw.rounded_rectangle((x, y, x + inner, y + h), radius=max(1, h//2), fill=(74, 161, 255, 255))

def _format_ts(seconds_value):
    total = max(0, int(seconds_value or 0))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def _load_font(size=14):
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]:
        try:
            return ImageFont.truetype(candidate, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _draw_time_badge(draw, x, y, text):
    font = _load_font(14)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x = 8
    pad_y = 3
    rect = (x, y, x + tw + pad_x * 2, y + th + pad_y * 2)
    draw.rounded_rectangle(rect, radius=8, fill=(235, 235, 235, 225), outline=(80, 80, 80, 255), width=1)
    draw.text((x + pad_x, y + pad_y - 1), text, font=font, fill=(20, 20, 20, 255))

def _draw_progress_bar(draw, x, y, w, h, fraction):
    draw.rounded_rectangle((x, y, x + w, y + h), radius=max(1, h//2), fill=(245, 245, 245, 230))
    inner = max(0, min(w, int(w * max(0.0, min(1.0, fraction)))))
    if inner > 0:
        draw.rounded_rectangle((x, y, x + inner, y + h), radius=max(1, h//2), fill=(74, 161, 255, 255))

def _choose_thumbnail_times(duration, seed_text):
    rng = random.Random(seed_text)
    if duration and duration > 40:
        for _ in range(20):
            left_ts = max(1, int(duration * rng.uniform(0.05, 0.25)))
            middle_ts = max(1, int(duration * rng.uniform(0.25, 0.75)))
            right_ts = max(1, int(duration * rng.uniform(0.75, 0.95)))
            if left_ts < middle_ts < right_ts:
                return left_ts, middle_ts, right_ts
        return max(1, int(duration * 0.15)), max(1, int(duration * 0.50)), max(1, int(duration * 0.85))
    return 5, 15, 25

def create_collage(video_path: Path, out_path: Path, media_info):
    duration = ffprobe_duration(video_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    left_ts, middle_ts, right_ts = _choose_thumbnail_times(duration, video_path.name)
    positions = [
        ("left", left_ts),
        ("middle", middle_ts),
        ("right", right_ts),
    ]

    hdr_vf = detect_hdr_filter(media_info)
    fallback_filters = [hdr_vf, "scale=1600:-1", ""]
    thumbs = []

    for idx, (slot, ts) in enumerate(positions, start=1):
        thumb = out_path.parent / f"_thumb_{slot}_{idx}.png"
        for vf in fallback_filters:
            cmd = ["ffmpeg", "-y", "-ss", str(ts), "-i", str(video_path), "-frames:v", "1"]
            if vf:
                cmd += ["-vf", vf]
            cmd += [str(thumb)]
            try:
                subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if thumb.exists() and thumb.stat().st_size > 0:
                    thumbs.append((slot, thumb, ts))
                    break
            except Exception:
                pass

    canvas_w, canvas_h = 700, 312
    collage = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))

    if not thumbs:
        draw = ImageDraw.Draw(collage)
        draw.rounded_rectangle((20, 20, canvas_w-20, canvas_h-20), radius=18, fill=(0, 0, 0, 180), outline=(245,245,245,255), width=3)
        draw.text((40, 40), "Thumbnail generation failed", fill=(255,255,255,255), font=_load_font(16))
        collage.save(out_path)
        return str(out_path)

    thumb_map = {slot: (thumb_path, ts) for slot, thumb_path, ts in thumbs}
    fallback = thumb_map.get("middle") or thumb_map.get("left") or thumb_map.get("right")
    for slot in ("left", "middle", "right"):
        if slot not in thumb_map and fallback:
            thumb_map[slot] = fallback

    panel_w, panel_h = 330, 185
    layout = [
        ("left", (10, 10)),
        ("middle", (185, 117)),
        ("right", (360, 10)),
    ]

    for slot, (px, py) in layout:
        thumb_path, ts = thumb_map[slot]
        try:
            im = Image.open(thumb_path).convert("RGBA")
        except Exception:
            im = Image.new("RGBA", (panel_w, panel_h), (20, 20, 20, 255))
        fitted = ImageOps.fit(im, (panel_w, panel_h), method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
        mask = Image.new("L", (panel_w, panel_h), 0)
        ImageDraw.Draw(mask).rounded_rectangle((0, 0, panel_w - 1, panel_h - 1), radius=18, fill=255)

        panel = Image.new("RGBA", (panel_w, panel_h), (0, 0, 0, 0))
        panel.paste(fitted, (0, 0), mask)
        draw = ImageDraw.Draw(panel)
        draw.rounded_rectangle((0, 0, panel_w - 1, panel_h - 1), radius=18, outline=(245,245,245,255), width=5)
        badge_y = panel_h - 29
        _draw_time_badge(draw, 15, badge_y, _format_ts(ts))
        if duration:
            _draw_progress_bar(draw, 15, panel_h - 15, panel_w - 30, 5, ts / duration)
        collage.alpha_composite(panel, (px, py))

    for _, pp, _ in thumbs:
        try:
            Path(pp).unlink()
        except Exception:
            pass

    collage.save(out_path)
    return str(out_path)



def redact_url_query(value):
    try:
        text = str(value or "")
        # Hide obvious API key style query values.
        text = re.sub(r'([?&](?:key|api_key|token)=)[^&]+', r'\1***REDACTED***', text, flags=re.I)
        return text
    except Exception:
        return ""

def upload_imgbox(collage_path, settings):
    api_key = (settings.get("packing_freeimage_api_key") or "").strip()
    if not api_key:
        return ""

    files = {"source": open(collage_path, "rb")}
    data = {
        "key": api_key,
        "action": "upload",
        "format": "json",
    }

    try:
        r = requests.post("https://freeimage.host/api/1/upload", data=data, files=files, timeout=180)
        payload = r.json() if r.content else {}
        image = payload.get("image", {}) if isinstance(payload, dict) else {}
        direct_url = (image.get("url") or "").strip()
        if direct_url:
            return f"[img]{direct_url}[/img]"
    except Exception:
        return ""
    finally:
        try:
            files["source"].close()
        except Exception:
            pass
    return ""
def resolve_thumbnail_code_second_pass(existing_code, settings):
    return existing_code or ""
def mediainfo_summary(video_path):
    try:
        out = subprocess.check_output(["mediainfo", str(video_path)], stderr=subprocess.STDOUT).decode("utf-8","replace")
        return out
    except Exception:
        return ""

def folderinfo_text(job_path):
    names = []
    for p in sorted(job_path.rglob("*")):
        if p.is_file():
            names.append(str(p.relative_to(job_path)))
    return "Files in job folder:\n" + "\n".join(names) + "\n"

def scaninfo_text(job_path):
    lines = []
    files = [p for p in sorted(job_path.rglob("*")) if p.is_file() and is_video(p)]
    total = len(files)
    for idx,p in enumerate(files, start=1):
        lines.append("=========================================================")
        lines.append("Bundle scan")
        lines.append(f"Job {idx} of {total}")
        lines.append("=========================================================")
        lines.append(f"Disc Label:     {p.name}")
        try:
            lines.append(f"Disc Size:      {p.stat().st_size:,} bytes")
        except Exception:
            lines.append("Disc Size:      0 bytes")
        lines.append("")
        lines.append(mediainfo_summary(p))
        lines.append("")
    return "\n".join(lines)

def template_text(job_name, header_name, groups, size_gb, password, nzb_name, imgbox_url, folderinfo, scaninfo):
    # Deprecated wrapper preserved for compatibility.
    parts = []
    parts.append("[SIZE=7][COLOR=rgb(41, 105, 176)][B][U]Media[/U][/B][/COLOR][/SIZE]")
    parts.append(f"[COLOR=rgb(209, 72, 65)][SIZE=6][SIZE=4]{job_name}[/SIZE][/SIZE][/COLOR]")
    parts.append("[SPOILER=click here for details]")
    parts.append("[SPOILER=click here for header and password details]")
    parts.append("[HIDETHANKS]")
    parts.append(f"Header: {header_name}")
    parts.append(f"Group: {groups}")
    parts.append(f"Size: {size_gb}")
    parts.append("")
    parts.append(f"[NZB]{nzb_name}[/NZB]")
    parts.append("")
    parts.append("Password:")
    parts.append(f"[CODE]{password}[/CODE]")
    parts.append("[/HIDETHANKS]")
    parts.append("[/SPOILER]")
    parts.append("[SPOILER=click here for additional job information]")
    parts.append("List of Bundle Contents:")
    parts.append("========================")
    parts.append(folderinfo.rstrip())
    parts.append("[/SPOILER]")
    parts.append("[/SPOILER]")
    if imgbox_url:
        parts.append(f"[img]{imgbox_url}[/img]")
    return "\n".join(parts)

def append_joblist(csv_path: Path, row):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["Job Name", "Header", "Password", "Source Size", "Estimated Payload", "Date"])
        w.writerow(row)

def run_cmd(cmd, cwd=None):
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = []
    for line in proc.stdout:
        out.append(line)
    rc = proc.wait()
    return rc, "".join(out)

def run_cmd_monitored(cmd, cwd=None, on_tick=None, tick_seconds=1):
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    out_lines = []
    last_tick = 0.0
    while True:
        line = proc.stdout.readline() if proc.stdout else ""
        if line:
            out_lines.append(line)
        now_ts = time.time()
        if on_tick and (now_ts - last_tick >= tick_seconds):
            try:
                on_tick()
            except Exception:
                pass
            last_tick = now_ts
        if proc.poll() is not None:
            # drain remaining
            if proc.stdout:
                rest = proc.stdout.read()
                if rest:
                    out_lines.append(rest)
            break
        time.sleep(0.2)
    return proc.returncode, "".join(out_lines)



def strip_dest_root_prefix(text: str, dest_root: str) -> str:
    prefix = str(dest_root or "/media/dest").rstrip("/") + "/"
    return (text or "").replace(prefix, "")



def _reset_directory_contents(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    for child in list(path.iterdir()):
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        except FileNotFoundError:
            pass

def _prepare_clean_packing_roots(pack_job_root: Path, output_files_root: Path):
    _reset_directory_contents(pack_job_root)
    _reset_directory_contents(output_files_root)

def media_type_for_job_path(job_path: Path):
    lower = str(job_path).lower()
    if "/tv" in lower or "season " in lower:
        return "tv"
    if "/movies" in lower:
        return "movie"
    return "packing"
def run_packing_job(job_id, source_path, settings):
    allowed_roots = build_allowed_roots(settings)
    assert_no_parent_traversal(source_path, "packing source")
    assert_path_within_roots(source_path, allowed_roots, "packing source")
    source = Path(source_path)
    job_name = source.name
    packed_root = Path(settings.get("packing_output_root") or str(Path(settings.get("dest_root","/media/dest")) / "_packed"))
    pack_job_root = packed_root / job_name
    output_files_root = packed_root / "output files" / job_name
    _prepare_clean_packing_roots(pack_job_root, output_files_root)

    try:
        current_job = next((j for j in list_packing_jobs(5000) if int(j.get("id", 0)) == int(job_id)), None)
        if not current_job or str(current_job.get("status", "")).lower() != "running":
            start_packing(job_id)
        add_packing_event(job_id, "stability", "Checking folder stability...", 2)
        delay = int(settings.get("packing_stability_delay","30") or 30)
        size1 = folder_size(source)
        time.sleep(min(delay, 120))
        size2 = folder_size(source)
        if size1 != size2:
            add_packing_event(job_id, "stability", f"Folder changed during check. Latest size: {round(size2/GB,2)} GB", 6)
        size_bytes = size2
        update_packing_job(job_id, size_bytes=size_bytes, output_root=str(pack_job_root), output_files_root=str(output_files_root))
        add_packing_event(job_id, "analysis", f"Calculating job details for {round(size_bytes/GB,2)} GB", 10)

        volume_bytes, est_parts = choose_volume_size(size_bytes) if str(settings.get("packing_auto_volume","true")).lower()=="true" else (int(settings.get("packing_manual_volume_mb","100"))*MB, math.ceil(size_bytes/max(1,int(settings.get("packing_manual_volume_mb","100"))*MB)))
        par2_pct = choose_par2_percent(size_bytes) if str(settings.get("packing_auto_par2","true")).lower()=="true" else int(settings.get("packing_manual_par2_percent","10") or 10)
        token = generate_archive_token(settings.get("packing_name_length","15"), settings.get("packing_name_fixed_tag","FS"), settings.get("packing_name_fixed_pos","4"))
        password = generate_password(settings.get("packing_password_prefix","NZBCave_"), settings.get("packing_password_length","24"))
        update_packing_job(job_id, archive_token=token, password=password, rar_volume_bytes=volume_bytes, rar_parts_estimate=est_parts, par2_percent=par2_pct)
        add_packing_event(job_id, "analysis", f"RAR target: ~{est_parts} parts, PAR2: {par2_pct}% redundancy", 15)

        # RAR step
        volume_mb = max(1, int(volume_bytes / MB))
        rar_base = pack_job_root / token
        rar_cmd = ["rar","a","-ma5","-m1","-rr-","-s-","-ep1"]
        if str(settings.get("packing_header_encrypt","true")).lower()=="true":
            rar_cmd.append(f"-hp{password}")
        else:
            rar_cmd.append(f"-p{password}")
        rar_cmd.append(f"-v{volume_mb}m")
        rar_cmd += [str(rar_base), str(source)]

        def rar_tick():
            rar_count = len(list(pack_job_root.glob(f"{token}*.rar")))
            step_pct = 0 if est_parts <= 0 else min(99, int((rar_count / max(1, est_parts)) * 100))
            overall = 15 + int(step_pct * 0.45)
            add_packing_event(job_id, "rar", f"RAR packing running: {rar_count} / {est_parts} parts completed ({step_pct}% of step)", overall)

        add_packing_event(job_id, "rar", f"Starting RAR packing for ~{est_parts} parts", 16)
        rar_started_ts = time.time()
        rc, rar_log = run_cmd_monitored(rar_cmd, on_tick=rar_tick)
        rar_elapsed = int(time.time() - rar_started_ts)
        (output_files_root/"packing.log").write_text(rar_log, encoding="utf-8", errors="replace")
        rar_files = sorted(str(pp) for pp in pack_job_root.glob(f"{token}*.rar"))
        if rc != 0 or not rar_files:
            raise RuntimeError("RAR failed")
        rar_total_bytes = sum(Path(x).stat().st_size for x in rar_files)
        update_packing_job(job_id, rar_size_bytes=rar_total_bytes, rar_parts_actual=len(rar_files), rar_time_seconds=rar_elapsed)
        add_packing_event(job_id, "rar", f"RAR packing complete: {len(rar_files)} parts written", 60)

        # PAR2 step
        par2_base = pack_job_root / token
        threads = settings.get("packing_par2_threads","4")
        mem = settings.get("packing_par2_memory_mb","1024")
        block = settings.get("packing_par2_block_size","0")
        est_par_bytes = max(1, int(sum(Path(x).stat().st_size for x in rar_files) * (par2_pct/100.0)))
        par_cmd = ["par2","create",f"-r{par2_pct}",f"-t{threads}",f"-m{mem}"]
        if str(block) not in ("","0","None"):
            par_cmd.append(f"-s{block}")
        par_cmd += [str(par2_base)+".par2"] + rar_files

        par_tick_state = {"ticks": 0, "last_bytes": -1}

        def par_tick():
            par_tick_state["ticks"] += 1
            par_files = list(pack_job_root.glob(f"{token}*.par2"))
            par_bytes = sum(pf.stat().st_size for pf in par_files if pf.exists())

            byte_pct = int((par_bytes / max(1, est_par_bytes)) * 100) if est_par_bytes > 0 else 0

            if par_bytes != par_tick_state["last_bytes"]:
                step_pct = max(1, min(99, byte_pct))
                par_tick_state["last_bytes"] = par_bytes
            else:
                step_pct = max(1, min(95, par_tick_state["ticks"] * 2))

            overall = 60 + int(step_pct * 0.20)
            add_packing_event(
                job_id,
                "par2",
                f"PAR2 running: {step_pct}% of step, {len(par_files)} parity files written, {round(par_bytes/(1024**2),2)} MB created",
                overall
            )

        add_packing_event(job_id, "par2", f"Starting PAR2 generation at {par2_pct}% redundancy", 61)
        par_started_ts = time.time()
        rc, par_log = run_cmd_monitored(par_cmd, on_tick=par_tick)
        par_elapsed = int(time.time() - par_started_ts)
        with open(output_files_root/"packing.log","a",encoding="utf-8",errors="replace") as f:
            f.write("\n\n=== PAR2 ===\n"+par_log)
        if rc != 0:
            raise RuntimeError("PAR2 failed")
        par_files = list(pack_job_root.glob(f"{token}*.par2"))
        par_total_bytes = sum(pf.stat().st_size for pf in par_files if pf.exists())
        update_packing_job(job_id, par2_size_bytes=par_total_bytes, par2_time_seconds=par_elapsed)
        add_packing_event(job_id, "par2", f"PAR2 generation complete: {len(par_files)} parity files written", 80)

        # Thumbnail + imgbox
        rep = largest_video(source)
        probe = detect_tags(str(rep)) if rep else {}
        add_packing_event(job_id, "thumbnail", "Generating thumbnail collage", 82)
        collage = create_collage(rep, output_files_root/"THUMBNAIL.png", probe) if rep else ""
        if rep:
            try:
                left_dbg, middle_dbg, right_dbg = _choose_thumbnail_times(ffprobe_duration(rep), rep.name)
                with open(output_files_root/"packing.log","a",encoding="utf-8",errors="replace") as f:
                    f.write("\n\n=== THUMBNAIL TIMESTAMPS ===\n")
                    f.write(f"LEFT   {_format_ts(left_dbg)}\n")
                    f.write(f"MIDDLE {_format_ts(middle_dbg)}\n")
                    f.write(f"RIGHT  {_format_ts(right_dbg)}\n")
            except Exception:
                pass
        add_packing_event(job_id, "thumbnail", "Thumbnail collage ready", 88)

        add_packing_event(job_id, "thumbnail_link", "Resolving thumbnail host link", 89)
        imgbox_url = upload_imgbox(collage, settings) if collage else ""
        with open(output_files_root/"packing.log","a",encoding="utf-8",errors="replace") as f:
            f.write("\n\n=== THUMBNAIL LINK ===\n")
            f.write(f"Collage: {collage}\n")
            f.write(f"Resolved thumbnail code: {redact_url_query(imgbox_url)}\n")
        update_packing_job(job_id, collage_path=collage, imgbox_url=imgbox_url)
        add_packing_event(job_id, "thumbnail_link", "Thumbnail link resolved" if imgbox_url else "No thumbnail host link resolved", 92)

        # Metadata
        add_packing_event(job_id, "metadata", "Writing output files", 94)
        video_files = [vp for vp in sorted(source.rglob("*")) if vp.is_file() and is_video(vp)]
        folderinfo = folderinfo_text(source).replace("", "")
        scaninfo = scaninfo_text(source).replace("", "")
        groups = choose_random_groups(settings)
        size_gb_str = f"{round(size_bytes/GB, 2)} GB"

        summary_blocks = []
        full_scan_inserted = False
        for vf in video_files:
            summary_text, fullscan_text = build_summary_and_fullscan(vf)
            title = vf.name
            summary_blocks.append(f"[SPOILER=click here for {title} summary:]")
            summary_blocks.append(summary_text.rstrip())
            if not full_scan_inserted:
                summary_blocks.append("")
                summary_blocks.append("[SPOILER=click here for full scan]")
                summary_blocks.append(fullscan_text.rstrip())
                summary_blocks.append("[/SPOILER]")
                full_scan_inserted = True
            summary_blocks.append("")
            summary_blocks.append("[/SPOILER]")

        parts = []
        headline_tag = "Media"
        m_head = re.search(r"\[([^\]]+?)\]", job_name)
        if m_head:
            headline_tag = m_head.group(1) + ":"
        parts.append(f"[SIZE=7][COLOR=rgb(41, 105, 176)][B][U]{headline_tag}[/U][/B][/COLOR][/SIZE]")
        parts.append(f"[COLOR=rgb(209, 72, 65)][SIZE=6][SIZE=4]{job_name}[/SIZE][/SIZE][/COLOR]")
        parts.append("[SPOILER=click here for details]")
        parts.append("[SPOILER=click here for header and password details]")
        parts.append("[HIDETHANKS]")
        parts.append(f"Header: {token}")
        parts.append(f"Group: {groups}")
        parts.append(f"Size: {size_gb_str}")
        parts.append("")
        parts.append(f"[NZB]{token}[/NZB]")
        parts.append("")
        parts.append("Password:")
        parts.append(f"[CODE]{password}[/CODE]")
        parts.append("[/HIDETHANKS]")
        parts.append("[/SPOILER]")
        parts.extend(summary_blocks)
        parts.append("[SPOILER=click here for additional job information]")
        parts.append("List of Bundle Contents:")
        parts.append("========================")
        parts.append(folderinfo.rstrip())
        parts.append("[/SPOILER]")
        parts.append("[/SPOILER]")
        if imgbox_url:
            parts.append(imgbox_url)
        template = "\n".join(parts)
        (output_files_root/"template.txt").write_text(template, encoding="utf-8", errors="replace")
        (output_files_root/"FOLDERINFO.txt").write_text(folderinfo, encoding="utf-8", errors="replace")
        (output_files_root/"SCANINFO.txt").write_text(scaninfo, encoding="utf-8", errors="replace")
        append_joblist(output_files_root/"JobList.csv", [job_name, token, password, size_gb_str, f"{round((size_bytes*(1+par2_pct/100))/GB,2)} GB", time.strftime("%B %d, %Y")])
        add_packing_event(job_id, "metadata", "Output files written successfully", 98)

        if str(settings.get("packing_delete_source_after_success","true")).lower() == "true":
            add_packing_event(job_id, "cleanup", "Deleting source folder after successful packing", 99)
            try:
                shutil.rmtree(source)
            except Exception as delete_error:
                raise RuntimeError(f"Packed successfully but failed to delete source folder: {delete_error}")

        finish_packing(job_id, True, "Packing complete")
        add_packing_event(job_id, "complete", "Packing complete", 100)
    except Exception as e:
        finish_packing(job_id, False, str(e))
        add_packing_event(job_id, "failed", str(e), None)
def start_packing_job_async(source_path, settings):
    source = Path(source_path)
    packed_root = Path(settings.get("packing_output_root") or str(Path(settings.get("dest_root","/media/dest")) / "_packed"))
    pack_job_root = packed_root / source.name
    output_files_root = packed_root / "output files" / source.name
    reconcile_orphaned_packing_jobs_in_process()
    existing_done_id = latest_successful_packing_job_id(str(source))
    if existing_done_id and not get_existing_active_packing_job_id(str(source)):
        return existing_done_id
    job_id = create_packing_job(str(source), source.name, str(pack_job_root), str(output_files_root))
    import threading

    def _runner():
        while True:
            current_settings = load_settings()
            try:
                max_jobs = max(1, int(current_settings.get("packing_max_concurrent_jobs", settings.get("packing_max_concurrent_jobs","1")) or 1))
            except Exception:
                max_jobs = 1
            reconcile_orphaned_packing_jobs_in_process()
            if try_claim_packing_slot(job_id, max_jobs):
                _mark_packing_job_active(job_id)
                break
            add_packing_event(job_id, "queued", f"Waiting for packing slot ({max_jobs} max concurrent jobs).", 0)
            time.sleep(1)
        try:
            run_packing_job(job_id, str(source), load_settings())
        finally:
            _mark_packing_job_inactive(job_id)

    threading.Thread(target=_runner, daemon=True).start()
    return job_id
