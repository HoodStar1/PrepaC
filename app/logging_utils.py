import json
import logging
import os
import sys
from datetime import datetime, timezone

from app.data_sanitizer import redact_sensitive_data


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_data(super().format(record))


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_sensitive_data(record.getMessage()),
        }
        for attr in ("event", "job_id", "job_kind", "path", "status", "phase"):
            value = getattr(record, attr, None)
            if value not in (None, ""):
                payload[attr] = redact_sensitive_data(str(value))
        if record.exc_info:
            payload["exc_info"] = redact_sensitive_data(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)


def setup_logging() -> None:
    level_name = str(os.environ.get("PREPAC_LOG_LEVEL", "INFO") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    use_json = str(os.environ.get("PREPAC_LOG_JSON", "false") or "false").strip().lower() in {"1", "true", "yes", "on"}

    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    if use_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(RedactingFormatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    root.addHandler(handler)
    logging.captureWarnings(True)
