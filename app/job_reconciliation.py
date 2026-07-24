"""
Background job reconciliation to prevent jobs from getting stuck in running state.
Runs periodically to detect and clean up orphaned jobs.
"""
import time
import logging
from app.db import get_conn
from app.timestamp_utils import (
    latest_local_timestamp,
    local_now,
    local_now_iso,
    parse_local_timestamp,
    recent_event_timestamps,
)

LOG = logging.getLogger(__name__)

# Track last reconciliation times to avoid excessive checking
LAST_RECONCILE = {
    "prepare": 0.0,
    "packing": 0.0,
    "posting": 0.0,
    "share": 0.0,
}

# Stale job thresholds (seconds since last activity)
STALE_THRESHOLDS = {
    "prepare": 30 * 60,      # 30 minutes
    "packing": 45 * 60,      # 45 minutes  
    "posting": 20 * 60,      # 20 minutes
    "share": 20 * 60,        # 20 minutes
}


def _parse_iso(ts):
    """Parse ISO timestamp safely."""
    return parse_local_timestamp(ts)


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


def _latest_row_activity(row, event_timestamps=()):
    values = list(event_timestamps)
    for field in ("started_at", "created_at"):
        try:
            values.append(row[field])
        except Exception:
            pass
    return latest_local_timestamp(values)


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
    status_outcomes=None,
):
    active_ids = _coerce_active_ids(active_job_ids)
    stale_threshold = STALE_THRESHOLDS.get(kind, 30 * 60)
    created_select = "j.created_at" if has_created_at else "NULL AS created_at"
    status_outcomes = dict(status_outcomes or {"running": "failed"})
    statuses = tuple(status_outcomes.keys())
    if not statuses:
        return 0
    status_placeholders = ",".join("?" for _ in statuses)

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT j.id, j.status, j.started_at, {created_select}
            FROM {job_table} j
            WHERE j.status IN ({status_placeholders})
            """,
            statuses,
        )
        rows = cur.fetchall()
        events_by_job = recent_event_timestamps(
            cur,
            event_table,
            event_fk,
            (row["id"] for row in rows),
        )

        recovered = 0
        now_dt = local_now()
        for row in rows:
            job_id = int(row["id"] or 0)
            if job_id in active_ids:
                continue

            last_activity = _latest_row_activity(
                row,
                events_by_job.get(job_id, ()),
            )
            if not last_activity:
                continue

            age_seconds = int((now_dt - last_activity).total_seconds())
            if age_seconds < stale_threshold:
                continue

            now_iso = local_now_iso()
            prior_status = str(row["status"] or "").lower()
            target_status = status_outcomes.get(prior_status)
            if not target_status:
                continue
            if target_status == "outcome_unknown":
                reason = (
                    f"Stale {prior_status} job requires manual reconciliation: no persisted activity "
                    f"for {age_seconds}s. The irreversible operation may have completed; verify its "
                    "destination before any explicit retry or fresh submission."
                )
            else:
                reason = f"Stale job recovered: no persisted activity for {age_seconds}s"
            assignments = ["status=?", "finished_at=?"]
            params = [target_status, now_iso]
            if has_message:
                assignments.append("message=?")
                params.append(reason)
            if clear_provider:
                assignments.extend(["provider_used=''", "provider_lock=''"])
            params.extend([job_id, prior_status])
            cur.execute(
                f"UPDATE {job_table} SET {', '.join(assignments)} WHERE id=? AND status=?",
                tuple(params),
            )
            if cur.rowcount > 0:
                event_phase = "outcome_unknown" if target_status == "outcome_unknown" else "recovered"
                cur.execute(
                    f"INSERT INTO {event_table}({event_fk}, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now_iso, event_phase, reason, None),
                )
                recovered += 1
                LOG.warning("Reconciled stale %s job %s as %s: %s", kind, job_id, target_status, reason)

        conn.commit()
        return recovered
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_exc:
                LOG.warning(
                    "Rollback failed while reconciling stale %s jobs: %s",
                    kind,
                    rollback_exc,
                )
        LOG.error("Error in reconcile_stale_%s_jobs: %s", kind, exc)
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                LOG.warning(
                    "Connection close failed while reconciling stale %s jobs: %s",
                    kind,
                    close_exc,
                )


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
        status_outcomes={"running": "failed", "finalizing": "outcome_unknown"},
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
        status_outcomes={"running": "failed", "finalizing": "outcome_unknown"},
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
        status_outcomes={"running": "failed", "finalizing": "outcome_unknown"},
    )


def reconcile_stale_share_jobs(active_job_ids=None):
    """Recover pre-upload work, but never auto-retry an ambiguous upload."""
    return _reconcile_stale_jobs(
        kind="share",
        job_table="share_jobs",
        event_table="share_job_events",
        event_fk="share_job_id",
        active_job_ids=active_job_ids,
        status_outcomes={"running": "failed", "uploading": "outcome_unknown"},
    )


def reconcile_abandoned_queued_jobs(reason="Recovered queued job left by a previous process"):
    """Fail non-persisted queue work once, while holding the startup lock.

    Queue workers are in-memory daemon threads.  A process crash therefore
    makes every pre-existing queued row orphaned; replaying it automatically
    could repeat an external effect.  The reconciliation-lock owner calls this
    before it starts accepting/reconciling new work.
    """
    specs = (
        ("prepare_jobs", "job_events", "job_id", False),
        ("packing_jobs", "packing_job_events", "packing_job_id", True),
        ("posting_jobs", "posting_job_events", "posting_job_id", True),
        ("share_jobs", "share_job_events", "share_job_id", True),
    )
    conn = get_conn(); cur = conn.cursor()
    changed = 0
    timestamp = local_now_iso()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for table, event_table, event_fk, has_message in specs:
            rows = cur.execute(f"SELECT id FROM {table} WHERE status='queued'").fetchall()
            for row in rows:
                job_id = int(row[0])
                message_set = ", message=?" if has_message else ""
                params = [timestamp]
                if has_message:
                    params.append(reason)
                params.append(job_id)
                cur.execute(
                    f"UPDATE {table} SET status='failed', finished_at=?{message_set} "
                    "WHERE id=? AND status='queued'",
                    tuple(params),
                )
                if cur.rowcount != 1:
                    continue
                cur.execute(
                    f"INSERT INTO {event_table}({event_fk}, timestamp, phase, message, percent) "
                    "VALUES (?, ?, 'recovered', ?, NULL)",
                    (job_id, timestamp, reason),
                )
                changed += 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def local_active_job_ids():
    """Return process-local active worker IDs grouped by workflow."""
    from app.jobs import ACTIVE_PREPARE_PROCS, ACTIVE_PREPARE_WORKERS
    from app.packing_core import PACKING_ACTIVE_JOB_IDS, PACKING_SCHEDULED_JOB_IDS
    from app.posting_core import POSTING_ACTIVE_JOB_IDS, POSTING_SCHEDULED_JOB_IDS
    from app.share_core import ACTIVE_SHARE_JOB_IDS, SHARE_SCHEDULED_JOB_IDS

    return {
        "prepare": set(ACTIVE_PREPARE_WORKERS) | set(ACTIVE_PREPARE_PROCS.keys()),
        "packing": set(PACKING_ACTIVE_JOB_IDS) | set(PACKING_SCHEDULED_JOB_IDS),
        "posting": set(POSTING_ACTIVE_JOB_IDS) | set(POSTING_SCHEDULED_JOB_IDS),
        "share": set(ACTIVE_SHARE_JOB_IDS) | set(SHARE_SCHEDULED_JOB_IDS),
    }


def wait_for_local_workers(timeout_seconds=10.0, poll_seconds=0.05):
    """Bounded graceful-drain wait; it never rewrites persisted job state."""
    deadline = time.monotonic() + max(0.0, float(timeout_seconds or 0.0))
    while True:
        active = local_active_job_ids()
        if not any(active.values()):
            return True, active
        if time.monotonic() >= deadline:
            return False, active
        time.sleep(max(0.01, min(0.5, float(poll_seconds or 0.05))))


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
            from app.packing_core import PACKING_ACTIVE_JOB_IDS, PACKING_SCHEDULED_JOB_IDS
            from app.posting_core import POSTING_ACTIVE_JOB_IDS, POSTING_SCHEDULED_JOB_IDS
            from app.share_core import ACTIVE_SHARE_JOB_IDS
            from app.jobs import ACTIVE_PREPARE_WORKERS, ACTIVE_PREPARE_PROCS
            
            # Get current active job IDs
            prepare_active = set(ACTIVE_PREPARE_WORKERS) | set(ACTIVE_PREPARE_PROCS.keys())
            packing_active = set(PACKING_ACTIVE_JOB_IDS) | set(PACKING_SCHEDULED_JOB_IDS)
            posting_active = set(POSTING_ACTIVE_JOB_IDS) | set(POSTING_SCHEDULED_JOB_IDS)
            share_active = set(ACTIVE_SHARE_JOB_IDS)
            
            # Reconcile stale jobs
            p_recovered = reconcile_stale_prepare_jobs(prepare_active)
            pk_recovered = reconcile_stale_packing_jobs(packing_active)
            po_recovered = reconcile_stale_posting_jobs(posting_active)
            sh_recovered = reconcile_stale_share_jobs(share_active)
            
            total = p_recovered + pk_recovered + po_recovered + sh_recovered
            if total > 0:
                LOG.info(f"Reconciliation complete: recovered {total} stale jobs (prepare={p_recovered}, packing={pk_recovered}, posting={po_recovered}, share={sh_recovered})")
        
        except Exception as e:
            LOG.error(f"Error in background_reconciliation_loop: {e}", exc_info=True)
