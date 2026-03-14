"""
Shared utilities for bioinformatics assembly API.
This module provides common Redis operations used by both the API and Celery workers.
"""
import os
import redis
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# Configure logging
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Job TTL (time to live) in seconds - jobs expire after 7 days
JOB_TTL = 7 * 24 * 60 * 60

# Heartbeat timeout in seconds - jobs without heartbeat for this long are considered stale
HEARTBEAT_TIMEOUT = 72000  # 20h

# Redis client singleton
_redis_client: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    """Get Redis client with lazy initialization and connection pooling."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )
    return _redis_client


def set_job(job_id: str, data: Dict[str, Any]) -> None:
    """Store job data in Redis."""
    r = get_redis()
    data['last_updated'] = datetime.now(timezone.utc).isoformat()
    data['last_heartbeat'] = datetime.now(timezone.utc).isoformat()
    r.hset(f"job:{job_id}", mapping=data)
    r.expire(f"job:{job_id}", JOB_TTL)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve job data from Redis."""
    r = get_redis()
    data = r.hgetall(f"job:{job_id}")
    return data if data else None


def update_job(job_id: str, **kwargs) -> None:
    """Update specific job fields in Redis."""
    r = get_redis()
    if kwargs:
        kwargs['last_updated'] = datetime.now(timezone.utc).isoformat()
        r.hset(f"job:{job_id}", mapping=kwargs)
        r.expire(f"job:{job_id}", JOB_TTL)


def update_heartbeat(job_id: str) -> None:
    """Update job heartbeat timestamp. Call this periodically during long-running tasks."""
    r = get_redis()
    r.hset(f"job:{job_id}", "last_heartbeat", datetime.now(timezone.utc).isoformat())
    r.expire(f"job:{job_id}", JOB_TTL)


def append_job_log(job_id: str, message: str) -> None:
    """Append a log message to job's log list."""
    r = get_redis()
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = f"[{timestamp}] {message}"
    r.rpush(f"job:{job_id}:logs", log_entry)
    r.expire(f"job:{job_id}:logs", JOB_TTL)
    logger.info(f"[Job {job_id}] {message}")


def get_job_logs(job_id: str) -> list:
    """Retrieve all logs for a job."""
    r = get_redis()
    return r.lrange(f"job:{job_id}:logs", 0, -1)


def get_all_jobs() -> list:
    """Get all job IDs (for monitoring)."""
    r = get_redis()
    keys = r.keys("job:*")
    job_keys = [key for key in keys if not key.endswith(":logs")]
    return [key.replace("job:", "") for key in job_keys]


def delete_job(job_id: str) -> None:
    """Delete a job and its logs from Redis."""
    r = get_redis()
    r.delete(f"job:{job_id}")
    r.delete(f"job:{job_id}:logs")


def is_job_stale(job: Dict[str, Any]) -> bool:
    """
    Check if a running job is stale (no heartbeat for too long).
    Returns True if the job is in 'running' status and hasn't had a heartbeat recently.
    """
    if job.get("status") != "running":
        return False
    
    last_heartbeat_str = job.get("last_heartbeat")
    if not last_heartbeat_str:
        # No heartbeat recorded, check last_updated instead
        last_heartbeat_str = job.get("last_updated")
    
    if not last_heartbeat_str:
        return True  # No timestamp at all, consider stale
    
    try:
        last_heartbeat = datetime.fromisoformat(last_heartbeat_str)
        now = datetime.now(timezone.utc)
        elapsed = (now - last_heartbeat).total_seconds()
        return elapsed > HEARTBEAT_TIMEOUT
    except (ValueError, TypeError):
        return True  # Invalid timestamp, consider stale
