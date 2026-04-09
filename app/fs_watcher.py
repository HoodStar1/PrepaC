import logging
from pathlib import Path

from app.cache_store import SCAN_CACHE
from app.metrics import inc

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except Exception:  # pragma: no cover
    FileSystemEventHandler = object
    Observer = None


LOG = logging.getLogger(__name__)
_OBSERVER = None
_WATCHED = set()


class _InvalidateHandler(FileSystemEventHandler):
    def __init__(self, prefixes):
        self.prefixes = tuple(prefixes)

    def on_any_event(self, event):
        for prefix in self.prefixes:
            SCAN_CACHE.invalidate_prefix(prefix)
        inc("prepac_fs_events", 1)


def start_watchers(settings):
    global _OBSERVER, _WATCHED
    if Observer is None:
        LOG.info("watchdog not available; file-system observers disabled")
        return False
    roots = {
        str(Path(settings.get("packing_watch_root") or settings.get("dest_root") or "/media/dest")),
        str(Path(settings.get("packing_output_root") or "/media/dest/_packed")),
    }
    roots = {r for r in roots if r}
    if _OBSERVER is not None and roots == _WATCHED:
        return True
    stop_watchers()
    observer = Observer()
    for root in sorted(roots):
        path = Path(root)
        if not path.exists():
            continue
        observer.schedule(_InvalidateHandler(("scan:packing", "scan:posting")), str(path), recursive=True)
    observer.daemon = True
    observer.start()
    _OBSERVER = observer
    _WATCHED = roots
    LOG.info("started file-system watchers for %s", ", ".join(sorted(_WATCHED)))
    return True


def stop_watchers():
    global _OBSERVER, _WATCHED
    observer = _OBSERVER
    _OBSERVER = None
    _WATCHED = set()
    if observer is None:
        return
    observer.stop()
    observer.join(timeout=5)
