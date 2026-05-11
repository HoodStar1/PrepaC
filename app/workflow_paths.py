from pathlib import Path


LEGACY_DEST_ROOT = "/media/dest"
LEGACY_PACKING_OUTPUT_ROOT = "/media/dest/_packed"
LEGACY_POSTING_POSTED_ROOT = "/media/dest/_posted"
LEGACY_POSTING_NZB_ROOT = "/media/dest/_nzb"
LEGACY_SHARE_IMPORT_ROOT = "/media/dest/_share/imports"


def _text(value) -> str:
    return str(value or "").strip()


def _path_text(value) -> str:
    value = _text(value)
    if not value:
        return ""
    return str(Path(value))


def _same_path(left, right) -> bool:
    return _path_text(left).rstrip("/\\") == _path_text(right).rstrip("/\\")


def prepare_root(settings) -> Path:
    """Destination root for prepared media. Keeps dest_root as the legacy key."""
    return Path(_text(settings.get("prepare_output_root")) or _text(settings.get("dest_root")) or LEGACY_DEST_ROOT)


def _prepare_root_changed(settings) -> bool:
    return not _same_path(prepare_root(settings), LEGACY_DEST_ROOT)


def _derived_root(settings, key: str, legacy_default: str, fallback: Path) -> Path:
    configured = _text(settings.get(key))
    if not configured:
        return Path(fallback)
    if _prepare_root_changed(settings) and _same_path(configured, legacy_default):
        return Path(fallback)
    return Path(configured)


def packing_watch_root(settings) -> Path:
    return _derived_root(settings, "packing_watch_root", LEGACY_DEST_ROOT, prepare_root(settings))


def packing_output_root(settings) -> Path:
    return _derived_root(settings, "packing_output_root", LEGACY_PACKING_OUTPUT_ROOT, prepare_root(settings) / "_packed")


def posting_watch_root(settings) -> Path:
    configured = _text(settings.get("posting_watch_root"))
    return Path(configured) if configured else packing_output_root(settings)


def posting_posted_root(settings) -> Path:
    return _derived_root(settings, "posting_posted_root", LEGACY_POSTING_POSTED_ROOT, prepare_root(settings) / "_posted")


def posting_nzb_root(settings) -> Path:
    return _derived_root(settings, "posting_nzb_root", LEGACY_POSTING_NZB_ROOT, prepare_root(settings) / "_nzb")


def share_watch_root(settings) -> Path:
    configured = _text(settings.get("share_watch_root"))
    return Path(configured) if configured else posting_posted_root(settings)


def share_import_root(settings) -> Path:
    return _derived_root(settings, "share_import_root", LEGACY_SHARE_IMPORT_ROOT, prepare_root(settings) / "_share" / "imports")


def effective_workflow_paths(settings) -> dict:
    return {
        "dest_root_effective": str(prepare_root(settings)),
        "packing_watch_root_effective": str(packing_watch_root(settings)),
        "packing_output_root_effective": str(packing_output_root(settings)),
        "posting_watch_root_effective": str(posting_watch_root(settings)),
        "posting_posted_root_effective": str(posting_posted_root(settings)),
        "posting_nzb_root_effective": str(posting_nzb_root(settings)),
        "share_watch_root_effective": str(share_watch_root(settings)),
        "share_import_root_effective": str(share_import_root(settings)),
    }


def settings_with_effective_workflow_paths(settings) -> dict:
    data = dict(settings or {})
    data.setdefault("posting_watch_root", "")
    data.setdefault("share_watch_root", "")
    data.update(effective_workflow_paths(data))
    return data


def all_workflow_roots(settings) -> list[Path]:
    paths = [
        prepare_root(settings),
        packing_watch_root(settings),
        packing_output_root(settings),
        posting_watch_root(settings),
        posting_posted_root(settings),
        posting_nzb_root(settings),
        share_watch_root(settings),
        share_import_root(settings),
    ]
    return list(dict.fromkeys(paths))
