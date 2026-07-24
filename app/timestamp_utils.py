"""Consistent timestamp handling for persisted job activity.

PrepaC historically stored local, offset-naive ISO timestamps. Some migration
paths briefly wrote offset-aware values. Convert both forms to local naive
datetimes before comparing them so mixed legacy rows remain safe to inspect.
"""

from datetime import datetime


_EVENT_SOURCES = {
    ("job_events", "job_id"),
    ("packing_job_events", "packing_job_id"),
    ("posting_job_events", "posting_job_id"),
    ("share_job_events", "share_job_id"),
}


def local_now() -> datetime:
    """Return the local wall-clock representation used by persisted job rows."""
    return datetime.now()


def local_now_iso() -> str:
    """Return a persisted job timestamp in the established local ISO format."""
    return local_now().isoformat(timespec="seconds")


def parse_local_timestamp(value) -> datetime | None:
    """Parse ISO input and normalize offset-aware values to local naive time."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def latest_local_timestamp(values) -> datetime | None:
    """Return the chronologically latest parseable timestamp."""
    parsed = [item for item in (parse_local_timestamp(value) for value in values) if item]
    return max(parsed) if parsed else None


def recent_event_timestamps(
    cursor,
    event_table: str,
    event_fk: str,
    job_ids,
    *,
    per_job_limit: int = 100,
):
    """Load bounded recent event candidates without ordering ISO text values."""
    source = (str(event_table), str(event_fk))
    if source not in _EVENT_SOURCES:
        raise ValueError("Unsupported job event source")
    normalized = set()
    for value in job_ids:
        try:
            job_id = int(value)
        except (TypeError, ValueError):
            continue
        if job_id > 0:
            normalized.add(job_id)
    normalized_ids = sorted(normalized)
    if not normalized_ids:
        return {}
    limit = max(1, min(100, int(per_job_limit)))
    timestamps = {job_id: [] for job_id in normalized_ids}
    for offset in range(0, len(normalized_ids), 400):
        chunk = normalized_ids[offset:offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = cursor.execute(
            f"""
            SELECT event_job_id, timestamp
            FROM (
                SELECT {event_fk} AS event_job_id, timestamp,
                       ROW_NUMBER() OVER (
                           PARTITION BY {event_fk}
                           ORDER BY id DESC
                       ) AS event_rank
                FROM {event_table}
                WHERE {event_fk} IN ({placeholders})
            ) AS recent_events
            WHERE event_rank <= ?
            """,
            (*chunk, limit),
        ).fetchall()
        for row in rows:
            timestamps.setdefault(int(row[0]), []).append(row[1])
    return timestamps
