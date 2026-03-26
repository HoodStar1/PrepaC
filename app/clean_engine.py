import re
import shutil
from datetime import datetime
from pathlib import Path
import os

from app.plex_clean_preview import media_root_storage_paths, path_size, container_to_host_user_path
YOUTUBE_ROOT_ALIASES = ["/media/youtube", "/media/Youtube Downloads", "/host_mnt/user/Youtube Downloads", "/mnt/user/Youtube Downloads"]

def _youtube_parent_folder_target(target_path: str):
    p = Path(str(target_path or ""))
    if not p.suffix:
        return None

    youtube_roots = [Path(x) for x in YOUTUBE_ROOT_ALIASES]
    rel = None
    for root in youtube_roots:
        try:
            rel = p.relative_to(root)
            break
        except Exception:
            continue

    if rel is None:
        return None

    # Safe rule:
    # channel/video_folder/file.mp4 -> delete video_folder
    # channel/file.mp4 -> do NOT delete channel, keep target as file
    if len(rel.parts) >= 3:
        parent = p.parent
        return str(parent) if parent and str(parent) not in YOUTUBE_ROOT_ALIASES else None
    return None
    low = text.lower()
    if not any(low.startswith(alias.lower() + "/") or low == alias.lower() for alias in YOUTUBE_ROOT_ALIASES):
        return None
    parent = p.parent
    if parent and str(parent) not in YOUTUBE_ROOT_ALIASES:
        return str(parent)
    return None

from app.clean_actions import log_clean_action
from app.db import load_settings
from app.path_guardrails import assert_no_parent_traversal, assert_path_within_roots, build_allowed_roots

def _safe_name(p: str) -> str:
    return p.replace(":", "_").replace("\\", "_").replace("/", "_").strip("_")

def move_to_recycle(targets, recycle_root):
    recycle_root.mkdir(parents=True, exist_ok=True)
    moved = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for p in targets:
        if not p.exists():
            continue
        dest = recycle_root / f"{timestamp}__{_safe_name(str(p))}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(p), str(dest))
        moved.append({"from": str(p), "to": str(dest)})
    return moved

def execution_paths(target_path: str):
    youtube_parent = _youtube_parent_folder_target(target_path)
    if youtube_parent:
        target_path = youtube_parent
    host_user = container_to_host_user_path(target_path)
    if host_user and host_user.exists():
        return [host_user]
    backing = media_root_storage_paths(target_path)
    if backing:
        return backing
    original = Path(target_path)
    if original.exists():
        return [original]
    return []



def _stat_allocated_bytes(path_obj: Path):
    try:
        st = path_obj.stat()
    except Exception:
        return 0, None
    allocated = int(getattr(st, "st_blocks", 0) or 0) * 512
    inode_key = (int(getattr(st, "st_dev", 0) or 0), int(getattr(st, "st_ino", 0) or 0))
    return allocated, inode_key

def _path_allocated_size(path_obj: Path):
    seen = set()
    total = 0
    try:
        if path_obj.is_file():
            allocated, inode_key = _stat_allocated_bytes(path_obj)
            if inode_key and inode_key not in seen:
                seen.add(inode_key)
                total += allocated
            return total
    except Exception:
        return 0

    try:
        for child in path_obj.rglob("*"):
            try:
                if child.is_file():
                    allocated, inode_key = _stat_allocated_bytes(child)
                    if inode_key and inode_key not in seen:
                        seen.add(inode_key)
                        total += allocated
            except Exception:
                pass
    except Exception:
        return 0
    return total

def _breakdown_totals(breakdown):
    logical = sum(int(x.get("bytes", 0) or 0) for x in breakdown)
    allocated = sum(int(x.get("allocated_bytes", 0) or 0) for x in breakdown)
    return logical, allocated

def _disk_free_delta(before, after):
    before_map = {str(x.get("mount_path")): int(x.get("free_bytes", 0) or 0) for x in (before or [])}
    after_map = {str(x.get("mount_path")): int(x.get("free_bytes", 0) or 0) for x in (after or [])}
    mounts = sorted(set(before_map.keys()) | set(after_map.keys()))
    out = []
    for mount in mounts:
        delta = after_map.get(mount, 0) - before_map.get(mount, 0)
        out.append({
            "mount_path": mount,
            "before_free_bytes": before_map.get(mount, 0),
            "after_free_bytes": after_map.get(mount, 0),
            "freed_bytes": delta,
        })
    return out

def _breakdown_for_paths(paths):
    total = 0
    breakdown = []
    for p in paths:
        logical_size = path_size(p) if p.exists() else 0
        allocated_size = _path_allocated_size(p) if p.exists() else 0
        total += logical_size
        breakdown.append({
            "storage_path": str(p),
            "bytes": logical_size,
            "logical_bytes": logical_size,
            "allocated_bytes": allocated_size,
        })
    return total, breakdown

def _unique_mount_roots(paths):
    mounts = []
    seen = set()
    for p in paths:
        p = Path(p)
        parts = p.parts
        if len(parts) >= 3 and parts[1] == "host_mnt":
            root = Path(*parts[:3])
        elif len(parts) >= 3 and parts[1] == "mnt":
            root = Path(*parts[:3])
        else:
            root = Path(p.anchor) if p.anchor else p
        sp = str(root)
        if sp not in seen:
            seen.add(sp)
            mounts.append(root)
    return mounts

def _free_space_snapshot(paths):
    snapshots = []
    for root in _unique_mount_roots(paths):
        try:
            usage = shutil.disk_usage(root)
            snapshots.append({
                "mount_path": str(root),
                "free_bytes": usage.free,
                "total_bytes": usage.total,
                "used_bytes": usage.used,
            })
        except Exception:
            pass
    return snapshots

def _is_season_like_folder(path_obj: Path) -> bool:
    name = path_obj.name.strip().lower()
    if name in {"specials", "season 00", "season 0", "s00"}:
        return True
    if re.match(r"^season\s*\d+$", name):
        return True
    if re.match(r"^s\d{1,2}$", name):
        return True
    return False

def delete_candidate(candidate, dry_run=True, use_recycle_bin=True, recycle_bin_root="/media/dest/.prepac_recycle"):
    settings = load_settings()
    allowed_roots = build_allowed_roots(settings)
    target = candidate["target_path"]
    effective_target = _youtube_parent_folder_target(target) or target
    if effective_target != target:
        details_hint = {"youtube_parent_folder_target": effective_target}
    else:
        details_hint = {}
    assert_no_parent_traversal(effective_target, "clean target")
    assert_path_within_roots(effective_target, allowed_roots, "clean target")
    breakdown_targets = media_root_storage_paths(effective_target)
    if not breakdown_targets:
        host_user = container_to_host_user_path(target)
        if host_user and host_user.exists():
            breakdown_targets = [host_user]
        elif Path(target).exists():
            breakdown_targets = [Path(target)]

    before_total, before_breakdown = _breakdown_for_paths(breakdown_targets)
    disk_free_before = _free_space_snapshot(breakdown_targets)

    success = True
    message = "dry run only"
    details = dict(candidate.get("details", {}))
    details.update(details_hint)

    extra_show_folder_remove = None
    if candidate.get("target_kind") == "season_folder":
        season_parent = Path(target).parent
        details["season_parent_show_path"] = str(season_parent)
        if season_parent.exists():
            season_dirs = [p for p in season_parent.iterdir() if p.is_dir() and _is_season_like_folder(p)]
            other_season_dirs = [p for p in season_dirs if p.name != Path(target).name]
            details["season_folder_names_in_show"] = [p.name for p in season_dirs]
            details["other_season_folder_names_in_show"] = [p.name for p in other_season_dirs]
            details["selected_season_is_only_season_folder"] = (len(other_season_dirs) == 0)
            if details["selected_season_is_only_season_folder"]:
                extra_show_folder_remove = season_parent

    if not dry_run:
        try:
            exec_targets = execution_paths(effective_target)
            if use_recycle_bin:
                assert_no_parent_traversal(recycle_bin_root, "recycle root")
                assert_path_within_roots(recycle_bin_root, allowed_roots, "recycle root")
                moved = move_to_recycle(exec_targets, Path(recycle_bin_root))
                if extra_show_folder_remove and extra_show_folder_remove.exists():
                    moved += move_to_recycle([extra_show_folder_remove], Path(recycle_bin_root))
                    details["removed_show_folder_because_only_season"] = str(extra_show_folder_remove)
                    message = "moved season folder and show folder to recycle bin"
                else:
                    message = "moved to recycle bin" if moved else "nothing moved"
                details["recycle_moves"] = moved
            else:
                removed = []
                for p in exec_targets:
                    if p.is_dir():
                        shutil.rmtree(p)
                        removed.append(str(p))
                    elif p.exists():
                        p.unlink()
                        removed.append(str(p))
                if extra_show_folder_remove and extra_show_folder_remove.exists():
                    shutil.rmtree(extra_show_folder_remove)
                    removed.append(str(extra_show_folder_remove))
                    details["removed_show_folder_because_only_season"] = str(extra_show_folder_remove)
                    message = "deleted season folder and show folder"
                else:
                    message = "deleted" if removed else "nothing deleted"
                details["deleted_paths"] = removed
        except Exception as e:
            success = False
            message = f"Delete error: {e}"

    after_total, after_breakdown = _breakdown_for_paths(breakdown_targets)
    disk_free_after = _free_space_snapshot(breakdown_targets)

    before_logical_total, before_allocated_total = _breakdown_totals(before_breakdown)
    after_logical_total, after_allocated_total = _breakdown_totals(after_breakdown)
    disk_free_delta = _disk_free_delta(disk_free_before, disk_free_after)
    actual_freed_bytes = sum(max(0, int(x.get("freed_bytes", 0) or 0)) for x in disk_free_delta)
    allocated_bytes_removed = max(0, before_allocated_total - after_allocated_total)
    logical_bytes_removed = max(0, before_logical_total - after_logical_total)

    details["folder_storage_before"] = {"total_bytes": before_total, "allocated_total_bytes": before_allocated_total, "locations": before_breakdown}
    details["folder_storage_after"] = {"total_bytes": after_total, "allocated_total_bytes": after_allocated_total, "locations": after_breakdown}
    details["disk_free_before"] = disk_free_before
    details["disk_free_after"] = disk_free_after
    details["disk_free_delta"] = disk_free_delta
    details["logical_bytes_removed"] = logical_bytes_removed
    details["allocated_bytes_removed"] = allocated_bytes_removed
    details["actual_freed_bytes"] = actual_freed_bytes
    details["touched_storage_paths"] = [x["storage_path"] for x in before_breakdown]

    log_clean_action(
        candidate["reason"],
        candidate["media_type"],
        candidate["target_path"],
        candidate["target_kind"],
        dry_run,
        success,
        before_total,
        before_breakdown,
        details,
        message,
    )
    return {
        "target_path": candidate["target_path"],
        "media_type": candidate["media_type"],
        "reason": candidate["reason"],
        "dry_run": dry_run,
        "success": success,
        "size_bytes": before_total,
        "logical_size_bytes": before_logical_total,
        "allocated_size_bytes": before_allocated_total,
        "actual_freed_bytes": actual_freed_bytes,
        "breakdown": before_breakdown,
        "message": message,
        "details": details,
        "folder_storage_before": {"total_bytes": before_total, "allocated_total_bytes": before_allocated_total, "locations": before_breakdown},
        "folder_storage_after": {"total_bytes": after_total, "allocated_total_bytes": after_allocated_total, "locations": after_breakdown},
        "disk_free_before": disk_free_before,
        "disk_free_after": disk_free_after,
        "disk_free_delta": disk_free_delta,
    }
