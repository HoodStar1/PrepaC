"""
Background job reconciliation to prevent jobs from getting stuck in running state.
Runs periodically to detect and clean up orphaned jobs.
"""
import time
import logging
from datetime import datetime
from app.db import get_conn

LOG = logging.getLogger(__name__)

# Track last reconciliation times to avoid excessive checking
LAST_RECONCILE = {
    "prepare": 0.0,
    "packing": 0.0,
    "posting": 0.0,
}

# Stale job thresholds (seconds since last activity)
STALE_THRESHOLDS = {
    "prepare": 30 * 60,      # 30 minutes
    "packing": 45 * 60,      # 45 minutes  
    "posting": 20 * 60,      # 20 minutes
}


def _parse_iso(ts):
    """Parse ISO timestamp safely."""
    try:
        return datetime.fromisoformat(ts) if ts else None
    except Exception:
        return None


def _latest_activity(job):
    """Get the most recent timestamp from a job's events or fields."""
    times = []
    for field in ("finished_at", "started_at", "created_at"):
        dt = _parse_iso(job.get(field))
        if dt:
            times.append(dt)
    # Check events for most recent activity
    for ev in (job.get("events") or []):
        dt = _parse_iso(ev.get("timestamp"))
        if dt:
            times.append(dt)
    return max(times) if times else None


def _coerce_active_ids(active_job_ids):
    ids = set()
    for value in active_job_ids or set():
        try:
            ids.add(int(value))
        except Exception:
            pass
    return ids


def _latest_row_activity(row):
    times = []
    for field in ("last_event_at", "started_at", "created_at"):
        try:
            value = row[field]
        except Exception:
            value = None
        dt = _parse_iso(value)
        if dt:
            times.append(dt)
    return max(times) if times else None


def _reconcile_stale_jobs(
    *,
    kind,
    job_table,
    event_table,
    event_fk,
    active_job_ids=None,
    has_created_at=True,
    has_message=True,
    clear_provider=False,
):
    active_ids = _coerce_active_ids(active_job_ids)
    stale_threshold = STALE_THRESHOLDS.get(kind, 30 * 60)
    created_select = "j.created_at" if has_created_at else "NULL AS created_at"
    message_set = ", message=?" if has_message else ""
    provider_set = ", provider_used=''" if clear_provider else ""

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT j.id, j.started_at, {created_select}, MAX(e.timestamp) AS last_event_at
            FROM {job_table} j
            LEFT JOIN {event_table} e ON e.{event_fk}=j.id
            WHERE j.status='running'
            GROUP BY j.id
            """
        )
        rows = cur.fetchall()

        recovered = 0
        now_dt = datetime.now()
        for row in rows:
            job_id = int(row["id"] or 0)
            if job_id in active_ids:
                continue

            last_activity = _latest_row_activity(row)
            if not last_activity:
                continue

            age_seconds = int((now_dt - last_activity).total_seconds())
            if age_seconds < stale_threshold:
                continue

            now_iso = datetime.now().isoformat(timespec="seconds")
            reason = f"Stale job recovered: no persisted activity for {age_seconds}s"
            params = [now_iso]
            if has_message:
                params.append(reason)
            params.append(job_id)
            cur.execute(
                f"UPDATE {job_table} SET status='failed', finished_at=?{message_set}{provider_set} "
                "WHERE id=? AND status='running'",
                tuple(params),
            )
            if cur.rowcount > 0:
                cur.execute(
                    f"INSERT INTO {event_table}({event_fk}, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now_iso, "recovered", reason, None),
                )
                recovered += 1
                LOG.warning("Recovered stale %s job %s: %s", kind, job_id, reason)

        conn.commit()
        conn.close()
        return recovered
    except Exception as e:
        LOG.error("Error in reconcile_stale_%s_jobs: %s", kind, e)
        return 0


def reconcile_stale_prepare_jobs(active_job_ids=None):
    """
    Mark prepare jobs as failed only after persisted activity has gone stale.
    In-memory active worker sets are process-local under gunicorn, so they are
    only an extra guard for this process, not the recovery signal.
    """
    return _reconcile_stale_jobs(
        kind="prepare",
        job_table="prepare_jobs",
        event_table="job_events",
        event_fk="job_id",
        active_job_ids=active_job_ids,
        has_created_at=False,
        has_message=False,
    )


def reconcile_stale_packing_jobs(active_job_ids=None):
    """
    Mark packing jobs as failed if they've been stuck in running state for too long
    and aren't actually being processed.
    """
    return _reconcile_stale_jobs(
        kind="packing",
        job_table="packing_jobs",
        event_table="packing_job_events",
        event_fk="packing_job_id",
        active_job_ids=active_job_ids,
    )


def reconcile_stale_posting_jobs(active_job_ids=None):
    """
    Mark posting jobs as failed if they've been stuck in running state for too long
    and aren't actually being processed.
    """
    return _reconcile_stale_jobs(
        kind="posting",
        job_table="posting_jobs",
        event_table="posting_job_events",
        event_fk="posting_job_id",
        active_job_ids=active_job_ids,
        clear_provider=True,
    )


def background_reconciliation_loop():
    """
    Continuous background task that periodically checks for and recovers stale jobs.
    Runs every 30 seconds to catch jobs that have been stuck.
    """
    LOG.info("Starting background job reconciliation loop")
    
    while True:
        try:
            time.sleep(30)  # Check every 30 seconds
            
            # Import here to avoid circular dependencies
            from app.packing_core import PACKING_ACTIVE_JOB_IDS
            from app.posting_core import POSTING_ACTIVE_JOB_IDS
            from app.jobs import ACTIVE_PREPARE_WORKERS, ACTIVE_PREPARE_PROCS
            
            # Get current active job IDs
            prepare_active = set(ACTIVE_PREPARE_WORKERS) | set(ACTIVE_PREPARE_PROCS.keys())
            packing_active = set(PACKING_ACTIVE_JOB_IDS)
            posting_active = set(POSTING_ACTIVE_JOB_IDS)
            
            # Reconcile stale jobs
            p_recovered = reconcile_stale_prepare_jobs(prepare_active)
            pk_recovered = reconcile_stale_packing_jobs(packing_active)
            po_recovered = reconcile_stale_posting_jobs(posting_active)
            
            total = p_recovered + pk_recovered + po_recovered
            if total > 0:
                LOG.info(f"Reconciliation complete: recovered {total} stale jobs (prepare={p_recovered}, packing={pk_recovered}, posting={po_recovered})")
        
        except Exception as e:
            LOG.error(f"Error in background_reconciliation_loop: {e}", exc_info=True)
