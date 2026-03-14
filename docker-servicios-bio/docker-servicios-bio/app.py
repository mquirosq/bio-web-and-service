"""
Bioinformatics Assembly API

FastAPI application for genome assembly and annotation.
Jobs are executed by Celery workers for proper distributed processing.
"""
from fastapi import FastAPI, UploadFile, HTTPException, Query
from fastapi.responses import FileResponse
import uuid
import shutil
from pathlib import Path
import os
from typing import Optional
import logging
from datetime import datetime, timezone, timedelta

# Import Celery tasks
from tasks import (
    run_assembly_task,
    run_annotation_task,
    celery_app
)

# Import shared utilities
from utils import (
    get_redis,
    set_job,
    get_job,
    update_job,
    append_job_log,
    get_job_logs,
    get_all_jobs,
    delete_job
)

WORKDIR = Path(os.getenv("WORKDIR_PATH", "/data/workdir"))
WORKDIR.mkdir(parents=True, exist_ok=True)

DEFAULT_MIN_CONTIG = int(os.getenv("DEFAULT_MIN_CONTIG", "500"))
DEFAULT_ILLUMINA_THREADS = int(os.getenv("DEFAULT_ILLUMINA_THREADS", "4"))
DEFAULT_ILLUMINA_MEMORY_GB = int(os.getenv("DEFAULT_ILLUMINA_MEMORY_GB", "8"))
DEFAULT_ONT_THREADS = int(os.getenv("DEFAULT_ONT_THREADS", "4"))
DEFAULT_ANNOTATION_THREADS = int(os.getenv("DEFAULT_ANNOTATION_THREADS", "4"))

# Configure logging after the workdir exists so file logging cannot fail at import time.
log_handlers = [logging.StreamHandler()]
try:
    log_handlers.append(logging.FileHandler(WORKDIR / "assembly_api.log"))
except OSError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bioinformatics Assembly API",
    description="API for genome assembly using Illumina and ONT technologies",
    version="2.0.0"
)

CANCELLABLE_STATUSES = {"pending", "running", "annotation_pending"}

# Verify Redis connection at startup
try:
    get_redis().ping()
    logger.info("Redis connection established")
except Exception as e:
    logger.warning(f"Redis connection failed at startup: {e}")


# ============== API Endpoints ==============

@app.post("/assembly/illumina")
async def assemble_illumina(
    r1: UploadFile,
    r2: UploadFile = None,
    min_contig: int = Query(DEFAULT_MIN_CONTIG, ge=0),
    threads: int = Query(DEFAULT_ILLUMINA_THREADS, ge=1),
    memory: int = Query(DEFAULT_ILLUMINA_MEMORY_GB, ge=1),
    assembler: str = None,
    annotate: bool = False,
    annotation_threads: int = Query(DEFAULT_ANNOTATION_THREADS, ge=1)
):
    """Submit Illumina assembly job"""
    job_id = str(uuid.uuid4())
    job_dir = WORKDIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # Save uploaded files
    r1_path = job_dir / "reads_R1.fastq.gz"
    with open(r1_path, "wb") as f:
        shutil.copyfileobj(r1.file, f)

    files = {"r1": str(r1_path)}

    if r2:
        r2_path = job_dir / "reads_R2.fastq.gz"
        with open(r2_path, "wb") as f:
            shutil.copyfileobj(r2.file, f)
        files["r2"] = str(r2_path)

    # Initialize job in Redis
    set_job(job_id, {
        "status": "pending",
        "stage": "assembly",
        "tech": "illumina",
        "retry_count": "0",
        "r1_path": str(r1_path),
        "r2_path": files.get("r2", ""),
        "threads": str(threads),
        "memory": str(memory),
        "min_contig": str(min_contig),
        "assembler": assembler or "",
        "auto_annotate": str(annotate),
        "annotation_threads": str(annotation_threads)
    })

    params = {
        "threads": threads,
        "memory": memory,
        "min_contig": min_contig,
        "assembler": assembler,
        "auto_annotate": annotate,
        "annotation_threads": annotation_threads
    }

    # Submit to Celery
    task = run_assembly_task.delay(job_id, "illumina", files, params)
    update_job(job_id, celery_task_id=task.id)
    append_job_log(job_id, f"Job submitted to Celery queue. Task ID: {task.id}")

    return {"job_id": job_id, "status": "pending", "celery_task_id": task.id}


@app.post("/assembly/ont")
async def assemble_ont(
    reads: UploadFile,
    min_contig: int = Query(DEFAULT_MIN_CONTIG, ge=0),
    threads: int = Query(DEFAULT_ONT_THREADS, ge=1),
    assembler: str = None,
    annotate: bool = False,
    annotation_threads: int = Query(DEFAULT_ANNOTATION_THREADS, ge=1)
):
    """Submit ONT assembly job"""
    job_id = str(uuid.uuid4())
    job_dir = WORKDIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # Save uploaded file
    reads_path = job_dir / "reads.fastq.gz"
    with open(reads_path, "wb") as f:
        shutil.copyfileobj(reads.file, f)

    files = {"reads": str(reads_path)}

    # Initialize job in Redis
    set_job(job_id, {
        "status": "pending",
        "stage": "assembly",
        "tech": "ont",
        "retry_count": "0",
        "reads_path": str(reads_path),
        "threads": str(threads),
        "min_contig": str(min_contig),
        "assembler": assembler or "",
        "auto_annotate": str(annotate),
        "annotation_threads": str(annotation_threads)
    })

    params = {
        "threads": threads,
        "min_contig": min_contig,
        "assembler": assembler,
        "auto_annotate": annotate,
        "annotation_threads": annotation_threads
    }

    # Submit to Celery
    task = run_assembly_task.delay(job_id, "ont", files, params)
    update_job(job_id, celery_task_id=task.id)
    append_job_log(job_id, f"Job submitted to Celery queue. Task ID: {task.id}")

    return {"job_id": job_id, "status": "pending", "celery_task_id": task.id}


@app.post("/jobs/{job_id}/retry")
async def retry_job(job_id: str):
    """Retry a failed job without re-uploading data"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") != "failed":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry job with status '{job.get('status')}'. Only failed jobs can be retried."
        )

    retry_count = int(job.get("retry_count", 0))
    if retry_count >= 3:
        raise HTTPException(
            status_code=400,
            detail="Maximum retry limit reached (3 attempts)."
        )

    job_dir = WORKDIR / job_id
    if not job_dir.exists():
        raise HTTPException(
            status_code=400,
            detail="Job directory no longer exists. Cannot retry."
        )

    tech = job.get("tech")
    stage = job.get("stage")

    if stage == "assembly":
        if tech == "illumina":
            r1_path = job.get("r1_path")
            r2_path = job.get("r2_path")

            if not r1_path or not os.path.exists(r1_path):
                raise HTTPException(status_code=400, detail="Original read files not found.")

            files = {"r1": r1_path}
            if r2_path and os.path.exists(r2_path):
                files["r2"] = r2_path

            params = {
                "threads": int(job.get("threads", DEFAULT_ILLUMINA_THREADS)),
                "memory": int(job.get("memory", DEFAULT_ILLUMINA_MEMORY_GB)),
                "min_contig": int(job.get("min_contig", DEFAULT_MIN_CONTIG)),
                "assembler": job.get("assembler") or None,
                "auto_annotate": job.get("auto_annotate") == "True",
                "annotation_threads": int(job.get("annotation_threads", DEFAULT_ANNOTATION_THREADS))
            }

        elif tech == "ont":
            reads_path = job.get("reads_path")

            if not reads_path or not os.path.exists(reads_path):
                raise HTTPException(status_code=400, detail="Original read files not found.")

            files = {"reads": reads_path}
            params = {
                "threads": int(job.get("threads", DEFAULT_ONT_THREADS)),
                "min_contig": int(job.get("min_contig", DEFAULT_MIN_CONTIG)),
                "assembler": job.get("assembler") or None,
                "auto_annotate": job.get("auto_annotate") == "True",
                "annotation_threads": int(job.get("annotation_threads", DEFAULT_ANNOTATION_THREADS))
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unknown technology: {tech}")

        new_retry_count = retry_count + 1
        update_job(
            job_id,
            status="pending",
            stage="assembly",
            retry_count=str(new_retry_count),
            message=f"Retry attempt {new_retry_count}/3"
        )
        append_job_log(job_id, f"Retrying assembly (attempt {new_retry_count}/3)")

        task = run_assembly_task.delay(job_id, tech, files, params)
        update_job(job_id, celery_task_id=task.id)

        return {
            "job_id": job_id,
            "status": "pending",
            "retry_count": new_retry_count,
            "celery_task_id": task.id,
            "message": f"Job resubmitted (attempt {new_retry_count}/3)"
        }

    elif stage == "annotation":
        contigs_path = job.get("result_file")

        if not contigs_path or not os.path.exists(contigs_path):
            raise HTTPException(status_code=400, detail="Contigs file not found.")

        threads = int(job.get("annotation_threads", job.get("threads", DEFAULT_ANNOTATION_THREADS)))
        new_retry_count = retry_count + 1

        update_job(
            job_id,
            status="annotation_pending",
            stage="annotation",
            retry_count=str(new_retry_count),
            message=f"Retry attempt {new_retry_count}/3"
        )
        append_job_log(job_id, f"Retrying annotation (attempt {new_retry_count}/3)")

        task = run_annotation_task.delay(job_id, contigs_path, threads)
        update_job(job_id, celery_task_id=task.id)

        return {
            "job_id": job_id,
            "status": "annotation_pending",
            "retry_count": new_retry_count,
            "celery_task_id": task.id,
            "message": f"Annotation resubmitted (attempt {new_retry_count}/3)"
        }

    else:
        raise HTTPException(status_code=400, detail=f"Unknown stage: {stage}")


@app.post("/annotation/bakta/existing/{job_id}")
async def annotate_bakta(job_id: str, threads: int = Query(DEFAULT_ANNOTATION_THREADS, ge=1)):
    """Submit Bakta annotation job for a completed assembly"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") not in ("assembled", "failed"):
        raise HTTPException(
            status_code=400,
            detail="Assembly must be completed or annotation failed before starting annotation"
        )

    if job.get("status") == "failed" and job.get("stage") == "annotation":
        update_job(job_id, retry_count="0")

    contigs_path = job.get("result_file")
    if not contigs_path or not os.path.exists(contigs_path):
        raise HTTPException(status_code=400, detail="Contigs file not found")

    task = run_annotation_task.delay(job_id, contigs_path, threads)
    update_job(
        job_id,
        annotation_threads=str(threads),
        stage="annotation",
        status="annotation_pending",
        celery_task_id=task.id
    )
    append_job_log(job_id, f"Annotation submitted to Celery. Task ID: {task.id}")

    return {"job_id": job_id, "status": "annotation_pending", "celery_task_id": task.id}


@app.post("/annotation/bakta/upload")
async def annotate_bakta_from_file(
    assembly: UploadFile,
    threads: int = Query(DEFAULT_ANNOTATION_THREADS, ge=1)
):
    """Submit Bakta annotation job directly from an uploaded assembly FASTA"""
    job_id = str(uuid.uuid4())
    job_dir = WORKDIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    safe_name = Path(assembly.filename or "assembly.fasta").name
    contigs_path = job_dir / safe_name
    with open(contigs_path, "wb") as f:
        shutil.copyfileobj(assembly.file, f)

    set_job(job_id, {
        "status": "annotation_pending",
        "stage": "annotation",
        "tech": "annotation_only",
        "retry_count": "0",
        "result_file": str(contigs_path),
        "annotation_threads": str(threads),
        "annotation_file": ""
    })
    append_job_log(job_id, f"Annotation-only job created. Assembly: {contigs_path}")

    task = run_annotation_task.delay(job_id, str(contigs_path), threads)
    update_job(job_id, celery_task_id=task.id)

    return {"job_id": job_id, "status": "annotation_pending", "celery_task_id": task.id}


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Check job status"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/jobs")
async def list_jobs(limit: int = 100, offset: int = 0):
    """List all jobs (for monitoring)"""
    job_ids = get_all_jobs()
    total = len(job_ids)

    paginated_ids = job_ids[offset:offset + limit]

    jobs_data = []
    for jid in paginated_ids:
        job = get_job(jid)
        if job:
            jobs_data.append({"job_id": jid, **job})

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "jobs": jobs_data
    }


@app.get("/jobs/{job_id}/logs")
async def get_job_logs_endpoint(job_id: str):
    """Get detailed logs for a job"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    logs = get_job_logs(job_id)

    return {
        "job_id": job_id,
        "status": job.get("status"),
        "tech": job.get("tech"),
        "logs": logs,
        "total_lines": len(logs)
    }


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a pending or running job"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    status = job.get("status")
    if status not in CANCELLABLE_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status '{status}'. Only pending or running jobs can be cancelled."
        )

    celery_task_id = job.get("celery_task_id")
    if celery_task_id:
        celery_app.control.revoke(celery_task_id, terminate=True)
        update_job(job_id, status="cancelled", celery_task_id="", message="Job cancelled by user")
        append_job_log(job_id, "Job cancelled by user")
        return {"job_id": job_id, "status": "cancelled"}

    raise HTTPException(status_code=400, detail="No Celery task ID found for this job")


@app.get("/assembly/{job_id}/download")
async def download_result(job_id: str):
    """Download assembly results"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] not in ("assembled", "annotated"):
        raise HTTPException(status_code=400, detail="Assembly not completed")

    result_file = job.get("result_file")
    if not result_file or not os.path.exists(result_file):
        raise HTTPException(status_code=400, detail="Result file not found")

    return FileResponse(
        result_file,
        media_type="application/octet-stream",
        filename=f"assembly_{job_id}.fasta"
    )


@app.get("/annotation/{job_id}/download")
async def download_annotation_result(
    job_id: str,
    file: Optional[str] = None,
    format: Optional[str] = None,
    archive: bool = False
):
    """Download Bakta annotation results"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") != "annotated":
        raise HTTPException(status_code=400, detail="Annotation not completed")

    annotation_file = job.get("annotation_file")
    if not annotation_file or not os.path.exists(annotation_file):
        raise HTTPException(status_code=400, detail="Annotation file not found")

    annotation_dir = Path(annotation_file).parent
    if not annotation_dir.exists():
        raise HTTPException(status_code=400, detail="Annotation directory not found")

    # Return ZIP of the entire annotation directory
    if archive or (file is None and format is None):
        zip_base = WORKDIR / job_id / f"annotation_{job_id}"
        zip_path = shutil.make_archive(str(zip_base), "zip", root_dir=str(annotation_dir))
        return FileResponse(zip_path, media_type="application/zip", filename=f"annotation_{job_id}.zip")

    # Return a specific file by exact name
    if file:
        target = (annotation_dir / file).resolve()
        if target.parent != annotation_dir.resolve():
            raise HTTPException(status_code=400, detail="Invalid file path")
        if not target.is_file():
            available = [p.name for p in annotation_dir.glob("*") if p.is_file()]
            raise HTTPException(status_code=404, detail=f"File not found. Available: {available}")
        return FileResponse(str(target), media_type="application/octet-stream", filename=target.name)

    # Return a file by format/extension
    if format:
        ext = format.lower().lstrip(".")
        base = Path(job.get("result_file", "")).stem or Path(annotation_file).stem
        preferred = annotation_dir / f"{base}.{ext}"
        if preferred.exists():
            return FileResponse(str(preferred), media_type="application/octet-stream", filename=preferred.name)
        candidates = sorted(annotation_dir.glob(f"*.{ext}"))
        if candidates:
            return FileResponse(str(candidates[0]), media_type="application/octet-stream", filename=candidates[0].name)
        available_exts = sorted({p.suffix.lstrip(".") for p in annotation_dir.iterdir() if p.is_file()})
        raise HTTPException(status_code=404, detail=f"No file with .{ext} found. Available: {available_exts}")

    raise HTTPException(status_code=400, detail="Specify 'archive=true', or 'file', or 'format'")


@app.delete("/jobs/old/{days}")
async def delete_old_jobs(days: int):
    """Delete jobs older than specified days"""
    if days < 0:
        raise HTTPException(status_code=400, detail="Days must be greater than or equal to 0")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    job_ids = get_all_jobs()
    deleted_count = 0

    for job_id in job_ids:
        job = get_job(job_id)
        if not job:
            continue
        last_updated_str = job.get("last_updated")
        if not last_updated_str:
            continue
        last_updated = datetime.fromisoformat(last_updated_str)
        if last_updated < cutoff:
            delete_job(job_id)
            job_dir = WORKDIR / job_id
            if job_dir.exists():
                shutil.rmtree(job_dir)
            deleted_count += 1

    return {"message": f"Deleted {deleted_count} jobs older than {days} days"}


@app.delete("/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    """Delete a job and its data"""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Cancel if running
    celery_task_id = job.get("celery_task_id")
    if celery_task_id:
        celery_app.control.revoke(celery_task_id, terminate=True)

    delete_job(job_id)

    job_dir = WORKDIR / job_id
    if job_dir.exists():
        shutil.rmtree(job_dir)

    return {"message": "Job deleted successfully"}


@app.delete("/jobs")
async def delete_all_jobs():
    """Delete all jobs and their data"""
    job_ids = get_all_jobs()
    for jid in job_ids:
        job = get_job(jid)
        if job:
            celery_task_id = job.get("celery_task_id")
            if celery_task_id:
                celery_app.control.revoke(celery_task_id, terminate=True)

        delete_job(jid)
        job_dir = WORKDIR / jid
        if job_dir.exists():
            shutil.rmtree(job_dir)

    return {"message": f"Deleted {len(job_ids)} jobs successfully"}


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker and monitoring"""
    redis_status = "disconnected"
    jobs_count = 0
    celery_status = "unknown"

    try:
        r = get_redis()
        r.ping()
        redis_status = "connected"
        jobs_count = len(get_all_jobs())
    except Exception as e:
        redis_status = f"error: {str(e)}"

    # Check Celery workers
    try:
        inspect = celery_app.control.inspect()
        active_workers = inspect.active()
        if active_workers:
            celery_status = f"connected ({len(active_workers)} workers)"
        else:
            celery_status = "no workers"
    except Exception as e:
        celery_status = f"error: {str(e)}"

    return {
        "status": "healthy" if redis_status == "connected" else "degraded",
        "workdir": str(WORKDIR),
        "workdir_exists": WORKDIR.exists(),
        "redis": redis_status,
        "celery": celery_status,
        "jobs_count": jobs_count
    }


@app.get("/workers")
async def get_workers():
    """Get information about Celery workers"""
    try:
        inspect = celery_app.control.inspect()
        return {
            "active": inspect.active(),
            "reserved": inspect.reserved(),
            "stats": inspect.stats()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect workers: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Bioinformatics Assembly API v2.0 (Celery-powered)",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "workers": "/workers",
            "illumina_assembly": "/assembly/illumina",
            "ont_assembly": "/assembly/ont",
            "list_jobs": "/jobs",
            "job_status": "/jobs/{job_id}",
            "job_logs": "/jobs/{job_id}/logs",
            "retry_job": "/jobs/{job_id}/retry",
            "cancel_job": "/jobs/{job_id}/cancel",
            "download_assembly": "/assembly/{job_id}/download",
            "download_annotation": "/annotation/{job_id}/download",
            "delete_job": "/jobs/{job_id}",
            "annotate": "/annotation/bakta/existing/{job_id}",
            "annotate_from_file": "/annotation/bakta/upload"
        }
    }
