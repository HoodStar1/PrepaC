import json, re, subprocess
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

def _norm_probe_text(value):
    return " ".join(re.sub(r"[\s_\-]+", " ", str(value or "").lower()).split())

def _mediainfo_sections(text):
    top_level = {"general", "video", "audio", "text", "menu", "chapters", "image", "other"}
    sections = []
    current_name = ""
    current_lines = []

    for raw in str(text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        header = line.split("#", 1)[0].strip().lower()
        if ":" not in line and header in top_level:
            if current_name:
                sections.append((current_name, current_lines))
            current_name = header
            current_lines = []
            continue
        if current_name:
            current_lines.append(line)

    if current_name:
        sections.append((current_name, current_lines))
    return sections

def _mediainfo_video_fields(text):
    fields = []
    for name, lines in _mediainfo_sections(text):
        if name != "video":
            continue
        for line in lines:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            fields.append((_norm_probe_text(key), _norm_probe_text(value)))
    return fields

def _mi_field_contains(fields, key_prefixes, needles):
    prefixes = tuple(_norm_probe_text(p) for p in key_prefixes)
    normalized_needles = tuple(_norm_probe_text(n) for n in needles)
    for key, value in fields:
        if not any(key.startswith(prefix) for prefix in prefixes):
            continue
        if any(needle and needle in value for needle in normalized_needles):
            return True
    return False

def _mi_has_field(fields, key_prefixes):
    prefixes = tuple(_norm_probe_text(p) for p in key_prefixes)
    return any(any(key.startswith(prefix) for prefix in prefixes) for key, _value in fields)

def detect_tags(path):
    path = str(Path(path))
    ff = ffprobe_json(path)
    mi = mediainfo_text(path)
    mil = mi.lower()

    width = height = None
    vcodec = acodec = channels = None

    for s in ff.get("streams", []):
        if s.get("codec_type") == "video" and width is None:
            # Use the larger of display dimensions vs coded dimensions so that
            # DV/anamorphic files with scaled display dimensions still resolve
            # to their true encoded resolution (e.g. coded_width=3840 wins over
            # a display width of 1920).
            w_raw = max(int(s.get("width") or 0), int(s.get("coded_width") or 0))
            h_raw = max(int(s.get("height") or 0), int(s.get("coded_height") or 0))
            width = w_raw or None
            height = h_raw or None
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

    # HDR grouping uses ffprobe structured data plus MediaInfo's Video section.
    # MediaInfo is never matched from the full output because General/filename
    # output includes General/filename fields that can falsely tag SDR files.
    _vstreams = [s for s in ff.get("streams", []) if s.get("codec_type") == "video"]

    def _side_data_texts():
        texts = []
        for s in _vstreams:
            for sd in s.get("side_data_list", []):
                try:
                    text = json.dumps(sd, sort_keys=True)
                except Exception:
                    text = str(sd)
                if text:
                    texts.append(_norm_probe_text(text))
        return texts

    _sd_texts = _side_data_texts()
    _transfers = {
        _norm_probe_text(s.get("color_transfer"))
        for s in _vstreams
        if s.get("color_transfer")
    }
    _mi_video_fields = _mediainfo_video_fields(mi)

    # Dolby Vision: DOVI config in side_data, or MediaInfo's Video HDR format.
    has_dv = (
        any("dovi" in t or "dolby vision" in t for t in _sd_texts)
        or _mi_field_contains(_mi_video_fields, ("hdr format",), ("dolby vision",))
    )

    # HLG: arib-std-b67 is the unambiguous ffprobe value for HLG transfer.
    has_hlg = (
        "arib std b67" in _transfers
        or _mi_field_contains(_mi_video_fields, ("hdr format", "transfer characteristics"), ("hlg", "arib std b67"))
    )

    # HDR10+: side_data SMPTE 2094-40 entry, or MediaInfo's Video HDR format.
    has_hdr10plus = (
        any("smpte2094" in t or "smpte 2094" in t or "hdr10+" in t or "hdr10 plus" in t for t in _sd_texts)
        or _mi_field_contains(
            _mi_video_fields,
            ("hdr format", "hdr format string"),
            ("hdr10+", "hdr10 plus", "smpte st 2094", "smpte 2094", "2094 40", "dynamic metadata"),
        )
    )

    # HDR10: PQ plus static metadata from ffprobe, or static HDR fields from
    # MediaInfo's Video section. PQ alone is shared with DV-only and HDR10Plus.
    _has_pq = (
        "smpte2084" in _transfers
        or "smpte 2084" in _transfers
        or _mi_field_contains(_mi_video_fields, ("transfer characteristics",), ("pq", "smpte st 2084", "smpte 2084"))
    )
    _has_mastering = any("mastering" in t or "content light" in t for t in _sd_texts)
    _mi_has_static_hdr = (
        _mi_field_contains(
            _mi_video_fields,
            ("hdr format", "hdr format string"),
            ("hdr10", "smpte st 2086", "smpte 2086", "st 2086"),
        )
        or _mi_has_field(
            _mi_video_fields,
            (
                "mastering display",
                "maximum content light",
                "maximum frame average light",
                "maximum frame-average light",
                "maxcll",
                "maxfall",
            ),
        )
    )
    has_hdr10 = (_has_pq and _has_mastering) or _mi_has_static_hdr
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

_HDR_BRACKET_PATTERN = re.compile(
    r"\[[^\]]*(?:dolby\s*vision|\bdovi\b|\bdv\b|hdr10\+|hdr10plus|\bhdr10\b|\bhdr\b|\bhlg\b)[^\]]*\]",
    re.IGNORECASE,
)

def merge_bracket_with_detected_hdr(bracket, info, source_type="movie"):
    bracket = str(bracket or "").strip()
    if not bracket:
        return build_bracket_from_detected(info, source_type)

    hdr_group = str((info or {}).get("hdr_group") or "").strip()
    if not hdr_group or _HDR_BRACKET_PATTERN.search(bracket):
        return bracket

    hdr_segment = f"[{hdr_group}]"
    first_part = re.search(r"\[[^\]]+\]", bracket)
    if not first_part:
        return f"{bracket}{hdr_segment}"
    insert_at = first_part.end()
    return f"{bracket[:insert_at]}{hdr_segment}{bracket[insert_at:]}"

def build_bracket_from_detected(info, source_type="movie"):
    tags = [t for t in info.get("detected_tags", []) if t]
    return "".join(f"[{t}]" for t in tags)
