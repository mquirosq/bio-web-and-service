import time
import uuid
from dataclasses import dataclass


@dataclass
class MockJob:
    job_id: str
    task_type: str
    behavior: str
    created_at: float
    duration: float


_jobs: dict[str, MockJob] = {}


def create_job(
    task_type: str,
    behavior: str,
    duration: float,
) -> MockJob:
    job = MockJob(
        job_id=f"mock-{uuid.uuid4().hex[:8]}",
        task_type=task_type,
        behavior=behavior,
        created_at=time.monotonic(),
        duration=duration,
    )

    _jobs[job.job_id] = job

    return job


def get_job(job_id: str) -> MockJob | None:
    return _jobs.get(job_id)


def get_status(job: MockJob) -> str:
    if job.behavior == "fail":
        return "failed"

    if job.behavior == "busy":
        return "busy"

    if job.behavior == "slow":
        elapsed = time.monotonic() - job.created_at

        if elapsed < job.duration:
            return "running"

        return "completed"

    return "completed"