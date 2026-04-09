import threading
import time


class TTLCache:
    def __init__(self):
        self._lock = threading.RLock()
        self._values = {}

    def get(self, key):
        with self._lock:
            item = self._values.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at < time.time():
                self._values.pop(key, None)
                return None
            return value

    def set(self, key, value, ttl_seconds: float):
        with self._lock:
            self._values[key] = (time.time() + max(0.0, float(ttl_seconds)), value)
            return value

    def invalidate(self, key):
        with self._lock:
            self._values.pop(key, None)

    def invalidate_prefix(self, prefix: str):
        with self._lock:
            for key in list(self._values.keys()):
                if str(key).startswith(prefix):
                    self._values.pop(key, None)


SCAN_CACHE = TTLCache()
