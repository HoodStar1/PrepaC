"""Filesystem authorization helpers for PrepaC workflows.

Paths supplied by a browser are identifiers, not authorization. Workflow
entry points use the operation-specific helpers in this module so a path that
is valid for one step is not automatically valid for a destructive step.
"""

from __future__ import annotations

import os
from pathlib import Path, PureWindowsPath
from typing import Iterable

from app.workflow_paths import (
    all_workflow_roots,
    packing_output_root,
    packing_watch_root,
    posting_nzb_root,
    posting_posted_root,
    posting_watch_root,
    prepare_root,
    share_import_root,
    share_watch_root,
)


ALIAS_ROOTS = {
    "/media/youtube": ["/media/Youtube Downloads"],
    "/media/Youtube Downloads": ["/media/youtube"],
    "/media/movies": ["/media/Movies"],
    "/media/Movies": ["/media/movies"],
    "/media/tv": ["/media/TV Shows"],
    "/media/TV Shows": ["/media/tv"],
    "/media/dest": ["/media/TBP/Jobs"],
    "/media/TBP/Jobs": ["/media/dest"],
}

# Configuration is deliberately absent: it is never a workflow data root.
ALLOWED_ROOT_SETTING_KEYS = [
    "tv_root", "movie_root", "youtube_root", "dest_root",
    "prepare_output_root", "packing_watch_root", "packing_output_root",
    "posting_posted_root", "posting_nzb_root", "posting_watch_root",
    "share_watch_root", "share_import_root", "recycle_bin_root",
]


def _text(value) -> str:
    return str(value or "").strip()


def _safe_resolve(value) -> Path:
    """Resolve an existing prefix without requiring the leaf to exist."""
    path = Path(_text(value))
    try:
        return path.resolve(strict=False)
    except (OSError, RuntimeError):
        return path.absolute()


def _dedupe_paths(values: Iterable) -> list[Path]:
    out = []
    seen = set()
    for value in values:
        if not _text(value):
            continue
        path = _safe_resolve(value)
        key = os.path.normcase(os.path.normpath(str(path)))
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out


def _with_aliases(values: Iterable) -> list[Path]:
    expanded = []
    for value in values:
        raw = _text(value)
        if not raw:
            continue
        expanded.append(raw)
        resolved = _safe_resolve(raw)
        expanded.extend(ALIAS_ROOTS.get(raw, ()))
        expanded.extend(ALIAS_ROOTS.get(str(resolved), ()))
    return _dedupe_paths(expanded)


def path_is_within(path_value, root_value, *, allow_root=True) -> bool:
    path = _safe_resolve(path_value)
    root = _safe_resolve(root_value)
    try:
        relative = path.relative_to(root)
    except (ValueError, OSError):
        return False
    return allow_root or bool(relative.parts)


def paths_overlap(left, right) -> bool:
    """Return True for equal paths and either parent/child direction."""
    return path_is_within(left, right) or path_is_within(right, left)


def assert_no_parent_traversal(path_value, label="path"):
    text = _text(path_value)
    if not text:
        raise RuntimeError(f"{label} is empty")
    # Check Windows-form paths on every platform as well as native paths.
    parts = set(Path(text).parts) | set(PureWindowsPath(text).parts)
    if ".." in parts:
        raise RuntimeError(f"{label} contains parent traversal: {path_value}")


def _is_linklike(path: Path) -> bool:
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    return bool(callable(is_junction) and is_junction())


def assert_no_symlinks_in_path(path_value, label="path", *, include_leaf=True):
    """Reject symlinks in every existing component without drive-root loops."""
    assert_no_parent_traversal(path_value, label)
    raw = Path(_text(path_value))
    try:
        raw = raw.absolute()
    except OSError:
        pass
    components = [raw, *raw.parents] if include_leaf else [*raw.parents]
    for component in components:
        try:
            if _is_linklike(component):
                raise RuntimeError(f"{label} contains symlink or junction component: {component}")
        except OSError as exc:
            raise RuntimeError(f"Could not validate {label}: {exc}") from exc


def assert_tree_has_no_symlinks(path_value, label="path tree", *, max_entries=1_000_000):
    """Reject symlink descendants before an archive or recursive mutation."""
    root = Path(_text(path_value))
    assert_no_symlinks_in_path(root, label)
    if not root.exists() or not root.is_dir():
        return
    checked = 0
    for current, dirnames, filenames in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in [*dirnames, *filenames]:
            checked += 1
            if checked > max(1, int(max_entries)):
                raise RuntimeError(f"{label} exceeds the safe entry limit")
            candidate = current_path / name
            try:
                if _is_linklike(candidate):
                    raise RuntimeError(f"{label} contains symlink or junction: {candidate}")
            except OSError as exc:
                raise RuntimeError(f"Could not validate {label}: {exc}") from exc


def protected_config_roots(settings) -> list[Path]:
    configured = [
        settings.get("config_root") if isinstance(settings, dict) else "",
        os.environ.get("PREPAC_CONFIG_DIR", ""),
    ]
    if os.name != "nt":
        configured.append("/config")
    return _with_aliases(configured)


def _setting_roots(settings, *keys) -> list[Path]:
    return _with_aliases(settings.get(key, "") for key in keys)


def operation_roots(settings, operation: str) -> list[Path]:
    """Return the data roots authorized for one workflow operation."""
    settings = settings or {}
    operation = _text(operation).lower()
    roots_by_operation = {
        "prepare_tv_source": lambda: _setting_roots(settings, "tv_root"),
        "prepare_movie_source": lambda: _setting_roots(settings, "movie_root"),
        "prepare_destination": lambda: _with_aliases([prepare_root(settings)]),
        "packing_source": lambda: _with_aliases([packing_watch_root(settings)]),
        "packing_output": lambda: _with_aliases([packing_output_root(settings)]),
        "posting_source": lambda: _with_aliases([posting_watch_root(settings)]),
        "posting_output": lambda: _with_aliases([posting_posted_root(settings)]),
        "posting_nzb": lambda: _with_aliases([posting_nzb_root(settings)]),
        "share_archive": lambda: _with_aliases(
            [share_watch_root(settings), share_import_root(settings), posting_nzb_root(settings)]
        ),
        "share_template": lambda: _with_aliases(
            [share_watch_root(settings), share_import_root(settings), posting_posted_root(settings)]
        ),
        "share_import": lambda: _with_aliases([share_import_root(settings)]),
        "clean_target": lambda: _setting_roots(settings, "tv_root", "movie_root", "youtube_root"),
        "clean_recycle": lambda: _setting_roots(settings, "recycle_bin_root"),
    }
    factory = roots_by_operation.get(operation)
    if factory is None:
        raise ValueError(f"Unknown path operation: {operation}")
    roots = factory()
    if not roots:
        raise RuntimeError(f"No roots are configured for {operation}")
    return roots


def assert_not_config_path(path_value, settings, label="path"):
    path = _safe_resolve(path_value)
    for config_root in protected_config_roots(settings or {}):
        if paths_overlap(path, config_root):
            raise RuntimeError(f"{label} overlaps the protected configuration directory")


def assert_path_within_roots(
    path_value, allowed_roots, label="path", *, allow_root=False,
    require_exists=False, require_directory=None,
):
    """Verify containment and reject root equality and symlink traversal."""
    assert_no_symlinks_in_path(path_value, label)
    path = _safe_resolve(path_value)
    roots = _dedupe_paths(allowed_roots)
    if not any(path_is_within(path, root, allow_root=allow_root) for root in roots):
        raise RuntimeError(f"{label} is outside its allowed roots")
    if require_exists and not path.exists():
        raise RuntimeError(f"{label} does not exist: {path_value}")
    if require_directory is True and path.exists() and not path.is_dir():
        raise RuntimeError(f"{label} is not a directory: {path_value}")
    if require_directory is False and path.exists() and not path.is_file():
        raise RuntimeError(f"{label} is not a regular file: {path_value}")
    return path


def assert_operation_path(
    path_value, settings, operation, label="path", *, allow_root=False,
    require_exists=False, require_directory=None,
):
    path = assert_path_within_roots(
        path_value, operation_roots(settings, operation), label,
        allow_root=allow_root, require_exists=require_exists,
        require_directory=require_directory,
    )
    assert_not_config_path(path, settings, label)
    return path


def assert_paths_disjoint(left, right, left_label="source", right_label="destination"):
    if paths_overlap(left, right):
        raise RuntimeError(f"{left_label} overlaps {right_label}")


def assert_operation_pair(
    source, destination, settings, source_operation, destination_operation, *,
    source_label="source", destination_label="destination", source_exists=True,
):
    source_path = assert_operation_path(
        source, settings, source_operation, source_label,
        require_exists=source_exists,
    )
    destination_path = assert_operation_path(
        destination, settings, destination_operation, destination_label,
    )
    assert_paths_disjoint(source_path, destination_path, source_label, destination_label)
    return source_path, destination_path


def is_path_within_roots(path_value, allowed_roots):
    """Compatibility predicate; unlike mutation guards it permits equality."""
    return any(path_is_within(path_value, root, allow_root=True) for root in allowed_roots)


def build_allowed_roots(settings):
    """Compatibility roots for read-only callers; config is excluded."""
    roots = [settings.get(key, "") for key in ALLOWED_ROOT_SETTING_KEYS]
    roots.extend(all_workflow_roots(settings))
    protected = protected_config_roots(settings)
    return [
        str(root) for root in _with_aliases(roots)
        if not any(paths_overlap(root, config_root) for config_root in protected)
    ]
