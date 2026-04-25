"""
Background job reconciliation to prevent jobs from getting stuck in running state.
Runs periodically to detect and clean up orphaned jobs.
"""
import time
import logging
from datetime import datetime, timedelta
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


def reconcile_stale_prepare_jobs(active_job_ids=None):
    """
    Mark prepare jobs as failed if they've been stuck in running state for too long
    and aren't actually being processed.
    """
    active_ids = {int(x) for x in (active_job_ids or set()) if str(x).isdigit()}
    stale_threshold = STALE_THRESHOLDS.get("prepare", 30 * 60)
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Find all running prepare jobs
        cur.execute("SELECT id, started_at FROM prepare_jobs WHERE status='running'")
        rows = cur.fetchall()
        
        recovered = 0
        now = datetime.now()
        
        for row in rows:
            job_id = int(row[0]) if row[0] else 0
            
            # Skip if there's an active worker processing this job
            if job_id in active_ids:
                continue
            
            # Check if job is stale
            started_at = _parse_iso(row[1]) if row[1] else None
            check_time = started_at
            
            if not check_time:
                continue
            
            age_seconds = int((now - check_time).total_seconds())
            if age_seconds < stale_threshold:
                continue
            
            # Job is stale and has no active worker - mark as failed
            try:
                cur.execute(
                    "UPDATE prepare_jobs SET status='failed', finished_at=? WHERE id=? AND status='running'",
                    (datetime.now().isoformat(timespec="seconds"), job_id)
                )
                if cur.rowcount > 0:
                    now_iso = datetime.now().isoformat(timespec="seconds")
                    reason = f"Stale job recovered: was running for {age_seconds}s with no active worker"
                    cur.execute(
                        "INSERT INTO job_events(job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                        (job_id, now_iso, "recovered", reason, None)
                    )
                    recovered += 1
                    LOG.warning(f"Recovered stale prepare job {job_id}: {reason}")
            except Exception as e:
                LOG.error(f"Error recovering prepare job {job_id}: {e}")
        
        conn.commit()
        conn.close()
        return recovered
    except Exception as e:
        LOG.error(f"Error in reconcile_stale_prepare_jobs: {e}")
        return 0


def reconcile_stale_packing_jobs(active_job_ids=None):
    """
    Mark packing jobs as failed if they've been stuck in running state for too long
    and aren't actually being processed.
    """
    active_ids = {int(x) for x in (active_job_ids or set()) if str(x).isdigit()}
    stale_threshold = STALE_THRESHOLDS.get("packing", 45 * 60)
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Find all running packing jobs
        cur.execute("SELECT id, started_at, created_at FROM packing_jobs WHERE status='running'")
        rows = cur.fetchall()
        
        recovered = 0
        now = datetime.now()
        
        for row in rows:
            job_id = int(row[0]) if row[0] else 0
            
            # Skip if there's an active worker processing this job
            if job_id in active_ids:
                continue
            
            # Check if job is stale
            started_at = _parse_iso(row[1]) if row[1] else None
            created_at = _parse_iso(row[2]) if row[2] else None
            check_time = started_at or created_at
            
            if not check_time:
                continue
            
            age_seconds = int((now - check_time).total_seconds())
            if age_seconds < stale_threshold:
                continue
            
            # Job is stale and has no active worker - mark as failed
            try:
                cur.execute(
                    "UPDATE packing_jobs SET status='failed', finished_at=? WHERE id=? AND status='running'",
                    (datetime.now().isoformat(timespec="seconds"), job_id)
                )
                if cur.rowcount > 0:
                    now_iso = datetime.now().isoformat(timespec="seconds")
                    reason = f"Stale job recovered: was running for {age_seconds}s with no active worker"
                    cur.execute(
                        "INSERT INTO packing_job_events(packing_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                        (job_id, now_iso, "recovered", reason, None)
                    )
                    recovered += 1
                    LOG.warning(f"Recovered stale packing job {job_id}: {reason}")
            except Exception as e:
                LOG.error(f"Error recovering packing job {job_id}: {e}")
        
        conn.commit()
        conn.close()
        return recovered
    except Exception as e:
        LOG.error(f"Error in reconcile_stale_packing_jobs: {e}")
        return 0


def reconcile_stale_posting_jobs(active_job_ids=None):
    """
    Mark posting jobs as failed if they've been stuck in running state for too long
    and aren't actually being processed.
    """
    active_ids = {int(x) for x in (active_job_ids or set()) if str(x).isdigit()}
    stale_threshold = STALE_THRESHOLDS.get("posting", 20 * 60)
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # Find all running posting jobs
        cur.execute("SELECT id, started_at, created_at FROM posting_jobs WHERE status='running'")
        rows = cur.fetchall()
        
        recovered = 0
        now = datetime.now()
        
        for row in rows:
            job_id = int(row[0]) if row[0] else 0
            
            # Skip if there's an active worker processing this job
            if job_id in active_ids:
                continue
            
            # Check if job is stale
            started_at = _parse_iso(row[1]) if row[1] else None
            created_at = _parse_iso(row[2]) if row[2] else None
            check_time = started_at or created_at
            
            if not check_time:
                continue
            
            age_seconds = int((now - check_time).total_seconds())
            if age_seconds < stale_threshold:
                continue
            
            # Job is stale and has no active worker - mark as failed
            try:
                cur.execute(
                    "UPDATE posting_jobs SET status='failed', finished_at=? WHERE id=? AND status='running'",
                    (datetime.now().isoformat(timespec="seconds"), job_id)
                )
                if cur.rowcount > 0:
                    now_iso = datetime.now().isoformat(timespec="seconds")
                    reason = f"Stale job recovered: was running for {age_seconds}s with no active worker"
                    cur.execute(
                        "INSERT INTO posting_job_events(posting_job_id, timestamp, phase, message, percent) VALUES (?, ?, ?, ?, ?)",
                        (job_id, now_iso, "recovered", reason, None)
                    )
                    recovered += 1
                    LOG.warning(f"Recovered stale posting job {job_id}: {reason}")
            except Exception as e:
                LOG.error(f"Error recovering posting job {job_id}: {e}")
        
        conn.commit()
        conn.close()
        return recovered
    except Exception as e:
        LOG.error(f"Error in reconcile_stale_posting_jobs: {e}")
        return 0


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
