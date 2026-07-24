import os
import threading
from app.db import get_conn
from app.subprocess_utils import terminate_process
from app.timestamp_utils import (
    latest_local_timestamp,
    local_now,
    local_now_iso,
    recent_event_timestamps,
)

ACTIVE_PREPARE_PROCS = {}
ACTIVE_PREPARE_WORKERS = set()
PREPARE_SHUTDOWN_EVENT = threading.Event()

def now(): return local_now_iso()

def create_job(
    media_type,
    source_path,
    dest_path="",
    idempotency_key=None,
    return_created=False,
    initial_event=None,
):
    """Create a queued Prepare job and, optionally, its first event atomically."""
    event_values = None
    if initial_event is not None:
        try:
            event_phase, event_message, event_percent = initial_event
        except (TypeError, ValueError) as exc:
            raise ValueError("initial_event must contain phase, message, and percent") from exc
        event_values = (str(event_phase), str(event_message), event_percent)
    conn = get_conn(); cur = conn.cursor()
    key = str(idempotency_key or "").strip() or None
    try:
        conn.execute("BEGIN IMMEDIATE")
        columns = {str(row[1]) for row in cur.execute("PRAGMA table_info(prepare_jobs)").fetchall()}
        supports_idempotency = "idempotency_key" in columns
        if key and supports_idempotency:
            row = cur.execute(
                "SELECT id FROM prepare_jobs WHERE (idempotency_key=? OR source_path=?) "
                "AND status IN ('queued','running','finalizing','outcome_unknown') ORDER BY id DESC LIMIT 1",
                (key, source_path),
            ).fetchone()
            if row:
                conn.commit()
                result = int(row[0])
                return (result, False) if return_created else result
        elif supports_idempotency:
            row = cur.execute(
                "SELECT id FROM prepare_jobs WHERE source_path=? "
                "AND status IN ('queued','running','finalizing','outcome_unknown') ORDER BY id DESC LIMIT 1",
                (source_path,),
            ).fetchone()
            if row:
                conn.commit()
                result = int(row[0])
                return (result, False) if return_created else result
        if supports_idempotency:
            cur.execute(
                "INSERT INTO prepare_jobs(media_type, status, source_path, dest_path, started_at, idempotency_key) VALUES (?, 'queued', ?, ?, ?, ?)",
                (media_type, source_path, dest_path, now(), key),
            )
        else:
            cur.execute(
                "INSERT INTO prepare_jobs(media_type, status, source_path, dest_path, started_at) VALUES (?, 'queued', ?, ?, ?)",
                (media_type, source_path, dest_path, now()),
            )
        job_id = cur.lastrowid
        if event_values is not None:
            cur.execute(
                "INSERT INTO job_events(job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, ?, ?, ?)",
                (job_id, now(), *event_values),
            )
        conn.commit()
        return (job_id, True) if return_created else job_id
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

def set_job_status(job_id, status, dest_path=None):
    """Update a prepare job without reviving a terminal or cancelled job.

    The copy engine only uses this to attach the validated destination to a job
    which has already been claimed.  Requiring the persisted state to still be
    ``running`` closes the cancel-between-claim-and-start race.
    """
    normalized = str(status or "").strip().lower()
    conn = get_conn(); cur = conn.cursor()
    try:
        if normalized == "running":
            if dest_path is None:
                cur.execute(
                    "UPDATE prepare_jobs SET status='running' WHERE id=? AND status='running'",
                    (int(job_id),),
                )
            else:
                cur.execute(
                    "UPDATE prepare_jobs SET status='running', dest_path=? WHERE id=? AND status='running'",
                    (dest_path, int(job_id)),
                )
        else:
            if dest_path is None:
                cur.execute(
                    "UPDATE prepare_jobs SET status=? WHERE id=? AND status NOT IN ('done','failed','cancelled','finalizing')",
                    (normalized, int(job_id)),
                )
            else:
                cur.execute(
                    "UPDATE prepare_jobs SET status=?, dest_path=? WHERE id=? AND status NOT IN ('done','failed','cancelled','finalizing')",
                    (normalized, dest_path, int(job_id)),
                )
        changed = cur.rowcount == 1
        conn.commit()
        return changed
    finally:
        conn.close()


def begin_prepare_finalization(job_id):
    """Cross the cancellation boundary before history/permission finalization."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE prepare_jobs SET status='finalizing' WHERE id=? AND status='running'",
            (int(job_id),),
        )
        changed = cur.rowcount == 1
        conn.commit()
        return changed
    finally:
        conn.close()


def mark_prepare_outcome_unknown(job_id, message):
    safe_message = str(message or "Prepare finalization outcome is unknown")[:1000]
    conn = get_conn(); cur = conn.cursor()
    try:
        timestamp = now()
        cur.execute(
            "UPDATE prepare_jobs SET status='outcome_unknown', finished_at=? "
            "WHERE id=? AND status='finalizing'",
            (timestamp, int(job_id)),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO job_events(job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, 'outcome_unknown', ?, NULL)",
                (int(job_id), timestamp, safe_message),
            )
        conn.commit()
        return changed
    finally:
        conn.close()


def acknowledge_prepare_outcome_unknown_for_resubmission(job_id):
    """Unblock a fresh Prepare submission after verified destination cleanup."""
    conn = get_conn(); cur = conn.cursor()
    try:
        message = "Unknown outcome manually reconciled; destination cleanup acknowledged and fresh submission permitted"
        cur.execute(
            "UPDATE prepare_jobs SET status='failed' WHERE id=? AND status='outcome_unknown'",
            (int(job_id),),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, 'manual_reconcile', ?, NULL)",
                (int(job_id), now(), message),
            )
        conn.commit()
        return changed
    finally:
        conn.close()

def add_job_event(job_id, phase, message, percent=None):
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (job_id, now(), phase, message, percent))
        cur.execute("DELETE FROM job_events WHERE job_id=? AND id NOT IN (SELECT id FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT 100)", (job_id, job_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fail_prepare_job_if_active(job_id, message, phase="failed"):
    """Terminalize a queued/running job and record why its worker stopped."""
    safe_message = str(message or "Prepare worker stopped unexpectedly")[:1000]
    safe_phase = str(phase or "failed")[:100]
    conn = get_conn(); cur = conn.cursor()
    try:
        timestamp = now()
        conn.execute("BEGIN IMMEDIATE")
        cur.execute(
            "UPDATE prepare_jobs SET status='failed', finished_at=? "
            "WHERE id=? AND status IN ('queued','running')",
            (timestamp, int(job_id)),
        )
        changed = cur.rowcount == 1
        if changed:
            cur.execute(
                "INSERT INTO job_events(job_id, timestamp, phase, message, percent) "
                "VALUES (?, ?, ?, ?, NULL)",
                (int(job_id), timestamp, safe_phase, safe_message),
            )
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def finish_job(job_id, success=True):
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE prepare_jobs SET status=?, finished_at=? WHERE id=? AND status IN ('running','finalizing')",
            ("done" if success else "failed", now(), job_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()





def register_prepare_worker(job_id):
    ACTIVE_PREPARE_WORKERS.add(int(job_id))

def unregister_prepare_worker(job_id):
    ACTIVE_PREPARE_WORKERS.discard(int(job_id))


def prepare_shutdown_requested():
    return PREPARE_SHUTDOWN_EVENT.is_set()


def prepare_should_stop(job_id):
    if PREPARE_SHUTDOWN_EVENT.is_set():
        return True
    return get_prepare_job_status(job_id).lower() in {
        "",
        "cancelled",
        "done",
        "failed",
        "outcome_unknown",
    }


def request_prepare_shutdown():
    """Ask local workers to stop without rewriting shared cross-worker state."""
    PREPARE_SHUTDOWN_EVENT.set()
    for proc in list(ACTIVE_PREPARE_PROCS.values()):
        try:
            terminate_process(proc)
        except Exception:
            pass
    return len(set(ACTIVE_PREPARE_WORKERS) | set(ACTIVE_PREPARE_PROCS.keys()))

def reconcile_prepare_running_jobs(reason="Recovered stale prepare slot"):
    # In-memory activity is process-local. The launcher enforces one Gunicorn
    # worker; the age threshold still protects direct/module callers.
    try:
        stale_min_age = max(300, int(str(os.environ.get("PREPAC_PREPARE_RECOVERY_MIN_AGE_SECONDS", "1800") or "1800")))
    except Exception:
        stale_min_age = 1800

    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, status, started_at
            FROM prepare_jobs
            WHERE status IN ('running','finalizing')
            """
        )
        rows = cur.fetchall()
        events_by_job = recent_event_timestamps(
            cur,
            "job_events",
            "job_id",
            (row["id"] for row in rows),
        )
        changed = 0
        now_dt = local_now()
        for row in rows:
            jid = int(row["id"])
            last_activity = latest_local_timestamp(
                [row["started_at"], *events_by_job.get(jid, [])]
            )
            if last_activity:
                age_seconds = int((now_dt - last_activity).total_seconds())
                if age_seconds < stale_min_age:
                    continue
            if jid in ACTIVE_PREPARE_WORKERS or jid in ACTIVE_PREPARE_PROCS:
                continue
            prior_status = str(row["status"] or "").lower()
            target_status = "outcome_unknown" if prior_status == "finalizing" else "failed"
            cur.execute(
                "UPDATE prepare_jobs SET status=?, finished_at=? WHERE id=? AND status=?",
                (target_status, now(), jid, prior_status),
            )
            if cur.rowcount:
                changed += 1
                if target_status == "outcome_unknown":
                    recovery_message = (
                        f"{reason} (stale > {stale_min_age}s); finalization outcome is unknown. "
                        "Verify and clean the destination before force retry."
                    )
                    phase = "outcome_unknown"
                else:
                    recovery_message = f"{reason} (stale > {stale_min_age}s)"
                    phase = "recovered"
                cur.execute(
                    "INSERT INTO job_events(job_id, timestamp, phase, message, percent) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (jid, now(), phase, recovery_message, None),
                )
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def try_claim_prepare_slot(job_id, max_jobs):
    conn = None
    try:
        reconcile_prepare_running_jobs()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT COUNT(*) FROM prepare_jobs WHERE status IN ('running','finalizing')")
        running = int(cur.fetchone()[0] or 0)
        if running >= int(max_jobs):
            conn.rollback()
            return False
        cur.execute("SELECT id FROM prepare_jobs WHERE status='queued' ORDER BY id ASC LIMIT 1")
        next_row = cur.fetchone()
        if not next_row or int(next_row[0]) != int(job_id):
            conn.rollback()
            return False
        cur.execute("UPDATE prepare_jobs SET status='running', started_at=? WHERE id=? AND status='queued'", (now(), job_id))
        claimed = cur.rowcount == 1
        conn.commit()
        return claimed
    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        import logging
        logging.getLogger(__name__).warning(f"Failed to claim prepare slot for job {job_id}: {e}")
        return False
    finally:
        if conn is not None:
            conn.close()

def list_jobs(limit=20):
    """Fetch jobs with recent events - optimized to avoid N+1 queries."""
    conn = get_conn(); cur = conn.cursor()
    try:
        # Fetch job list
        cur.execute("SELECT * FROM prepare_jobs ORDER BY id DESC LIMIT ?", (limit,))
        jobs = [dict(r) for r in cur.fetchall()]
        if not jobs:
            return jobs

        job_ids = [j["id"] for j in jobs]

        # Fetch all recent events for these jobs in a single query
        placeholders = ",".join("?" * len(job_ids))
        cur.execute(f"""
            SELECT job_id, phase, message, percent, timestamp, id
            FROM job_events
            WHERE job_id IN ({placeholders})
            ORDER BY job_id DESC, id DESC
        """, job_ids)
        all_events = cur.fetchall()
    finally:
        conn.close()
    
    # Group events by job_id (maintaining order - most recent first per job)
    events_by_job = {}
    for event in all_events:
        job_id = event["job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 10:  # Keep only last 10
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs and set latest phase/message/percent
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
        if j["events"]:
            j["phase"] = j["events"][0]["phase"]
            j["message"] = j["events"][0]["message"]
            j["percent"] = j["events"][0]["percent"]
        else:
            j["phase"] = ""
            j["message"] = ""
            j["percent"] = None
    
    return jobs


def list_jobs_by_status(statuses, limit=500):
    """Fetch jobs by status with recent events - optimized to avoid N+1 queries."""
    wanted = [str(s).strip().lower() for s in (statuses or []) if str(s).strip()]
    if not wanted:
        return []
    
    placeholders = ",".join("?" for _ in wanted)
    conn = get_conn(); cur = conn.cursor()
    try:
        # Fetch job list
        cur.execute(
            f"SELECT * FROM prepare_jobs WHERE lower(status) IN ({placeholders}) "
            "ORDER BY id DESC LIMIT ?",
            tuple(wanted) + (int(limit),),
        )
        jobs = [dict(r) for r in cur.fetchall()]
        if not jobs:
            return jobs

        job_ids = [j["id"] for j in jobs]

        # Fetch all recent events for these jobs in a single query
        event_placeholders = ",".join("?" * len(job_ids))
        cur.execute(f"""
            SELECT job_id, phase, message, percent, timestamp, id
            FROM job_events
            WHERE job_id IN ({event_placeholders})
            ORDER BY job_id DESC, id DESC
        """, job_ids)
        all_events = cur.fetchall()
    finally:
        conn.close()
    
    # Group events by job_id (maintaining order - most recent first per job)
    events_by_job = {}
    for event in all_events:
        job_id = event["job_id"]
        if job_id not in events_by_job:
            events_by_job[job_id] = []
        if len(events_by_job[job_id]) < 20:  # Keep last 20 for this function
            events_by_job[job_id].append({
                "phase": event["phase"],
                "message": event["message"],
                "percent": event["percent"],
                "timestamp": event["timestamp"]
            })
    
    # Attach events to jobs and set latest phase/message/percent
    for j in jobs:
        j["events"] = events_by_job.get(j["id"], [])
        if j["events"]:
            j["phase"] = j["events"][0]["phase"]
            j["message"] = j["events"][0]["message"]
            j["percent"] = j["events"][0]["percent"]
        else:
            j["phase"] = ""
            j["message"] = ""
            j["percent"] = None
    
    return jobs


def interrupt_running_prepare_jobs(reason="Interrupted by container shutdown", recovery=False):
    if not recovery:
        # Shutdown is cooperative.  Do not mark shared jobs failed while their
        # owning worker (possibly in another Gunicorn process) can still finish.
        return request_prepare_shutdown()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM prepare_jobs WHERE status='running'")
    rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        phase = "recovered" if recovery else "shutdown"
        message = ("Recovered after restart: previous container exited during job execution"
                   if recovery else reason)
        cur.execute("UPDATE prepare_jobs SET status='failed', finished_at=?, message=? WHERE id=?",
                    (now(), message, row["id"]))
        cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                    (row["id"], now(), phase, message, row.get("percent")))
    conn.commit(); conn.close()
    return len(rows)


def get_prepare_job_status(job_id):
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM prepare_jobs WHERE id=?", (job_id,))
        row = cur.fetchone()
        return str(row[0]) if row else ""
    finally:
        conn.close()

def register_prepare_proc(job_id, proc):
    ACTIVE_PREPARE_PROCS[int(job_id)] = proc

def unregister_prepare_proc(job_id, proc=None):
    current = ACTIVE_PREPARE_PROCS.get(int(job_id))
    if proc is None or current is proc:
        ACTIVE_PREPARE_PROCS.pop(int(job_id), None)

def cancel_prepare_job(job_id, reason="Cancelled by user"):
    proc = ACTIVE_PREPARE_PROCS.get(int(job_id))
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("UPDATE prepare_jobs SET status='cancelled', finished_at=? WHERE id=? AND status IN ('queued','running')", (now(), job_id))
        changed = cur.rowcount > 0
        if changed:
            cur.execute("INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                        (job_id, now(), 'cancelled', reason, None))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    if changed and proc is not None:
        try:
            terminate_process(proc)
        except Exception:
            pass
    return changed
