import re
import shutil
from datetime import datetime
from pathlib import Path

from app.plex_clean_preview import media_root_storage_paths, path_size, container_to_host_user_path
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

def _breakdown_for_paths(paths):
    total = 0
    breakdown = []
    for p in paths:
        size = path_size(p) if p.exists() else 0
        total += size
        breakdown.append({"storage_path": str(p), "bytes": size})
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
    assert_no_parent_traversal(target, "clean target")
    assert_path_within_roots(target, allowed_roots, "clean target")
    breakdown_targets = media_root_storage_paths(target)
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
            exec_targets = execution_paths(target)
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

    details["folder_storage_before"] = {"total_bytes": before_total, "locations": before_breakdown}
    details["folder_storage_after"] = {"total_bytes": after_total, "locations": after_breakdown}
    details["disk_free_before"] = disk_free_before
    details["disk_free_after"] = disk_free_after
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
        "breakdown": before_breakdown,
        "message": message,
        "details": details,
        "folder_storage_before": {"total_bytes": before_total, "locations": before_breakdown},
        "folder_storage_after": {"total_bytes": after_total, "locations": after_breakdown},
        "disk_free_before": disk_free_before,
        "disk_free_after": disk_free_after,
    }
