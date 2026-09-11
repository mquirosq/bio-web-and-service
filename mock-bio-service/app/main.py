from fastapi import FastAPI, HTTPException, Query, Response

from .config import DEFAULT_DURATION, get_behavior
from .jobs import create_job, get_job, get_status
from .mock_data import (
    annotation_json,
    assembly_fasta,
    invalid_json,
)

app = FastAPI(title="COCOS Mock Bio Service")


def log(message: str):
    """Print a message with the mock service prefix."""
    print(f"[MOCK] {message}", flush=True)


def create_mock_job(
    task_type: str,
    behavior: str | None,
    initial_status: str = "pending",
):
    try:
        behavior = get_behavior(behavior)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )

    job = create_job(
        task_type=task_type,
        behavior=behavior,
        duration=DEFAULT_DURATION,
    )

    log(
        f"CREATE {task_type:<18} "
        f"job={job.job_id} | "
        f"behavior={behavior} | "
        f"duration={DEFAULT_DURATION}s | "
        f"initial_status={initial_status}"
    )

    return {
        "job_id": job.job_id,
        "status": initial_status,
    }


@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    job = get_job(job_id)

    if job is None:
        log(f"STATUS {job_id} | NOT FOUND")

        raise HTTPException(
            status_code=404,
            detail="Mock job not found",
        )

    status = get_status(job)

    log(
        f"STATUS {job_id} | "
        f"type={job.task_type} | "
        f"behavior={job.behavior} | "
        f"status={status}"
    )

    if status == "busy":
        return Response(
            content='{"status":"busy"}',
            status_code=503,
            media_type="application/json",
        )

    return {
        "status": status,
    }


@app.post("/assembly/ont")
def assembly_ont(
    annotate: bool = False,
    behavior: str | None = Query(default=None),
):
    log(
        f"REQUEST assembly_ont | "
        f"annotate={annotate} | "
        f"behavior={behavior}"
    )

    return create_mock_job(
        "assembly_ont",
        behavior,
    )


@app.post("/assembly/illumina")
def assembly_illumina(
    annotate: bool = False,
    behavior: str | None = Query(default=None),
):
    log(
        f"REQUEST assembly_illumina | "
        f"annotate={annotate} | "
        f"behavior={behavior}"
    )

    return create_mock_job(
        "assembly_illumina",
        behavior,
    )


@app.post("/annotation/bakta/upload")
def annotation_bakta_upload(
    threads: int = 4,
    behavior: str | None = Query(default=None),
):
    log(
        f"REQUEST annotation_bakta_upload | "
        f"threads={threads} | "
        f"behavior={behavior}"
    )

    return create_mock_job(
        "annotation",
        behavior,
        initial_status="running",
    )


@app.post("/annotation/bakta/existing/{job_id}")
def annotation_bakta_existing(
    job_id: str,
    threads: int = 4,
    behavior: str | None = Query(default=None),
):
    log(
        f"REQUEST annotation_bakta_existing | "
        f"source_job={job_id} | "
        f"threads={threads} | "
        f"behavior={behavior}"
    )

    original_job = get_job(job_id)

    if original_job is None:
        log(
            f"ANNOTATION source job {job_id} | "
            f"NOT FOUND"
        )

        raise HTTPException(
            status_code=404,
            detail="Original mock job not found",
        )

    # Same reason as above: annotation should immediately
    # enter the polling phase.
    return create_mock_job(
        "annotation",
        behavior,
        initial_status="running",
    )


@app.get("/assembly/{job_id}/download")
def download_assembly(job_id: str):
    job = get_job(job_id)

    if job is None:
        log(
            f"DOWNLOAD assembly | "
            f"job={job_id} | "
            f"NOT FOUND"
        )

        raise HTTPException(
            status_code=404,
        )

    status = get_status(job)

    log(
        f"DOWNLOAD assembly | "
        f"job={job_id} | "
        f"status={status} | "
        f"behavior={job.behavior}"
    )

    if status != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Job is not completed: {status}",
        )

    if job.behavior == "invalid":
        log(
            f"DOWNLOAD assembly | "
            f"job={job_id} | "
            f"returning INVALID FASTA"
        )

        return Response(
            content=b"this is not a valid FASTA file",
            media_type="application/octet-stream",
        )

    log(
        f"DOWNLOAD assembly | "
        f"job={job_id} | "
        f"returning valid FASTA"
    )

    return Response(
        content=assembly_fasta(),
        media_type="application/octet-stream",
    )


@app.get("/annotation/{job_id}/download")
def download_annotation(
    job_id: str,
    format: str = "json",
):
    job = get_job(job_id)

    if job is None:
        log(
            f"DOWNLOAD annotation | "
            f"job={job_id} | "
            f"NOT FOUND"
        )

        raise HTTPException(
            status_code=404,
        )

    status = get_status(job)

    log(
        f"DOWNLOAD annotation | "
        f"job={job_id} | "
        f"status={status} | "
        f"behavior={job.behavior} | "
        f"format={format}"
    )

    if status != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Job is not completed: {status}",
        )

    if format != "json":
        raise HTTPException(
            status_code=400,
            detail="Mock service only supports JSON",
        )

    if job.behavior == "invalid":
        log(
            f"DOWNLOAD annotation | "
            f"job={job_id} | "
            f"returning INVALID JSON"
        )

        return invalid_json()

    log(
        f"DOWNLOAD annotation | "
        f"job={job_id} | "
        f"returning valid JSON"
    )

    return annotation_json()