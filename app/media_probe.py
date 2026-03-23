import json, subprocess
from pathlib import Path

def _run(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=120).decode("utf-8", errors="replace")
    except Exception:
        return ""

def ffprobe_json(path):
    raw = _run(["ffprobe","-v","quiet","-print_format","json","-show_format","-show_streams",path])
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}

def mediainfo_text(path):
    return _run(["mediainfo", path])

def _resolution_from_dimensions(width, height):
    w = int(width or 0)
    h = int(height or 0)
    if w >= 3840 or h >= 2100:
        return "2160p"
    if w >= 1920 or h >= 1000:
        return "1080p"
    if w >= 1280 or h >= 700:
        return "720p"
    return None

def _audio_base(acodec, mi_lower):
    if "dts-hd ma" in mi_lower or "master audio" in mi_lower:
        return "DTS-HD MA"
    if "truehd" in mi_lower:
        return "TrueHD"
    if "e-ac-3" in mi_lower or "eac3" in mi_lower:
        return "EAC3"
    if "ac-3" in mi_lower or acodec == "ac3":
        return "AC3"
    if acodec == "aac":
        return "AAC"
    if acodec == "dts":
        return "DTS"
    return (acodec or "").upper() if acodec else None

def detect_tags(path):
    path = str(Path(path))
    ff = ffprobe_json(path)
    mi = mediainfo_text(path)
    mil = mi.lower()

    width = height = None
    vcodec = acodec = channels = None

    for s in ff.get("streams", []):
        if s.get("codec_type") == "video" and width is None:
            width = s.get("width")
            height = s.get("height")
            vcodec = (s.get("codec_name") or "").lower()
        if s.get("codec_type") == "audio" and acodec is None:
            acodec = (s.get("codec_name") or "").lower()
            ch = s.get("channels")
            if ch == 8:
                channels = "7_1"
            elif ch == 6:
                channels = "5_1"
            elif ch == 2:
                channels = "2_0"
            elif ch:
                channels = str(ch)

    resolution = _resolution_from_dimensions(width, height)

    # HDR grouping
    has_dv = "dolby vision" in mil
    has_hlg = "hlg" in mil
    has_hdr10plus = "hdr10+" in mil
    has_hdr10 = ("hdr10" in mil) or any((s.get("color_transfer") or "").lower() == "smpte2084" for s in ff.get("streams", []) if s.get("codec_type") == "video")
    if has_hlg:
        hdr_group = "HLG"
    elif has_dv and has_hdr10plus:
        hdr_group = "DV HDR10Plus"
    elif has_dv and has_hdr10:
        hdr_group = "DV HDR10"
    elif has_dv:
        hdr_group = "DV"
    elif has_hdr10plus:
        hdr_group = "HDR10Plus"
    elif has_hdr10:
        hdr_group = "HDR10"
    else:
        hdr_group = None

    # source tag from audio class
    lossless = any(x in mil for x in ["dts-hd ma", "master audio", "truehd", "flac", "pcm"])
    lossy = any(x in mil for x in ["e-ac-3", "eac3", "ac-3", "aac", "dts"]) or acodec in {"aac","ac3","eac3","dts","mp3","opus","vorbis"}
    audio_class = "lossless" if lossless else ("lossy" if lossy else None)

    source_tag = None
    if resolution:
        if audio_class == "lossless":
            source_tag = f"Bluray-{resolution} Remux"
        elif audio_class == "lossy":
            source_tag = f"WEBDL-{resolution}"
        else:
            source_tag = resolution

    audio_base = _audio_base(acodec, mil)
    has_atmos = "atmos" in mil
    audio_tag = None
    if audio_base and channels:
        audio_tag = f"{audio_base}{' Atmos' if has_atmos else ''} {channels}"
    elif audio_base:
        audio_tag = f"{audio_base}{' Atmos' if has_atmos else ''}"

    if vcodec in {"hevc","h265","x265"}:
        codec_tag = "HEVC" if audio_class == "lossless" else "h265"
    elif vcodec in {"h264","avc1","x264"}:
        codec_tag = "h264"
    else:
        codec_tag = (vcodec or "").upper() if vcodec else None

    detected = [x for x in [source_tag, hdr_group, audio_tag, codec_tag] if x]

    return {
        "path": path,
        "width": width,
        "height": height,
        "resolution": resolution,
        "audio_class": audio_class,
        "audio_codec": audio_base,
        "audio_channels": channels,
        "source_tag": source_tag,
        "hdr_group": hdr_group,
        "audio_tag": audio_tag,
        "video_codec": codec_tag,
        "detected_tags": detected,
        "raw_ffprobe": ff,
        "raw_mediainfo": mi,
        "atmos": has_atmos,
    }

def build_bracket_from_detected(info, source_type="movie"):
    tags = [t for t in info.get("detected_tags", []) if t]
    return "".join(f"[{t}]" for t in tags)
