import logging
import os
import threading
import time
from pathlib import Path

from app.cache_store import SCAN_CACHE
from app.metrics import inc
from app.workflow_paths import packing_output_root, packing_watch_root, posting_watch_root

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
        try:
            self.min_interval = max(0.25, float(os.environ.get("PREPAC_FS_EVENT_DEBOUNCE_SECONDS", "2.0") or "2.0"))
        except Exception:
            self.min_interval = 2.0
        self._lock = threading.Lock()
        self._last_invalidated = -self.min_interval
        self._pending_events = 0

    def on_any_event(self, event):
        event_count = 0
        now = time.monotonic()
        with self._lock:
            self._pending_events += 1
            if (now - self._last_invalidated) < self.min_interval:
                return
            event_count = self._pending_events
            self._pending_events = 0
            self._last_invalidated = now
        for prefix in self.prefixes:
            SCAN_CACHE.invalidate_prefix(prefix)
        inc("prepac_fs_events", event_count)


def start_watchers(settings):
    global _OBSERVER, _WATCHED
    if Observer is None:
        LOG.info("watchdog not available; file-system observers disabled")
        return False
    roots = {
        str(packing_watch_root(settings)),
        str(packing_output_root(settings)),
        str(posting_watch_root(settings)),
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
