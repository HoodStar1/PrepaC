import threading
from collections import defaultdict
from typing import Dict, Iterable


_LOCK = threading.RLock()
_COUNTERS = defaultdict(float)
_GAUGES = {}
_HIST = defaultdict(lambda: {"count": 0, "sum": 0.0})


def _metric_key(name: str, labels: Dict[str, str] | None = None) -> tuple[str, tuple[tuple[str, str], ...]]:
    clean = tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items()))
    return name, clean


def inc(name: str, value: float = 1.0, **labels):
    with _LOCK:
        _COUNTERS[_metric_key(name, labels)] += float(value)


def set_gauge(name: str, value: float, **labels):
    with _LOCK:
        _GAUGES[_metric_key(name, labels)] = float(value)


def observe(name: str, value: float, **labels):
    with _LOCK:
        bucket = _HIST[_metric_key(name, labels)]
        bucket["count"] += 1
        bucket["sum"] += float(value)


def labels_to_text(items: Iterable[tuple[str, str]]) -> str:
    items = list(items)
    if not items:
        return ""
    return "{" + ",".join(f'{k}="{v}"' for k, v in items) + "}"


def render_prometheus() -> str:
    lines = []
    with _LOCK:
        for (name, labels), value in sorted(_COUNTERS.items()):
            lines.append(f"{name}_total{labels_to_text(labels)} {value}")
        for (name, labels), value in sorted(_GAUGES.items()):
            lines.append(f"{name}{labels_to_text(labels)} {value}")
        for (name, labels), bucket in sorted(_HIST.items()):
            lines.append(f"{name}_count{labels_to_text(labels)} {bucket['count']}")
            lines.append(f"{name}_sum{labels_to_text(labels)} {bucket['sum']}")
    return "\n".join(lines) + "\n"
