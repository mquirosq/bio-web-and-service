"""
Celery tasks for bioinformatics assembly and annotation.
This module handles all long-running jobs in a distributed manner.
"""
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from pathlib import Path
import subprocess
import shutil
import os
import traceback
import logging

# Import shared utilities
from utils import (
    get_redis,
    get_job,
    update_job,
    update_heartbeat,
    append_job_log,
    get_all_jobs,
    is_job_stale,
    REDIS_URL,
    JOB_TTL,
    HEARTBEAT_TIMEOUT
)

# Configure logging
logger = logging.getLogger(__name__)

# Workdir configuration
WORKDIR = Path(os.getenv("WORKDIR_PATH", "/data/workdir"))

# Task timeout settings (in seconds)
ASSEMBLY_SOFT_TIMEOUT = int(os.getenv("ASSEMBLY_SOFT_TIMEOUT", "72000"))  # 20 hours soft limit
ASSEMBLY_HARD_TIMEOUT = int(os.getenv("ASSEMBLY_HARD_TIMEOUT", "75000"))  # 20h 50min hard limit
ANNOTATION_SOFT_TIMEOUT = int(os.getenv("ANNOTATION_SOFT_TIMEOUT", "36000"))  # 10 hours soft limit
ANNOTATION_HARD_TIMEOUT = int(os.getenv("ANNOTATION_HARD_TIMEOUT", "39000"))  # 10h 50min hard limit

# Create Celery app
celery_app = Celery(
    "bio_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Task settings
    task_acks_late=True,  # Acknowledge after task completes (not before)
    task_reject_on_worker_lost=True,  # Requeue if worker dies
    task_track_started=True,
    # Worker settings
    worker_prefetch_multiplier=1,  # Only fetch 1 task at a time per worker
    worker_concurrency=int(os.getenv("CELERY_CONCURRENCY", "2")),
    # Result backend settings
    result_expires=7 * 24 * 60 * 60,  # 7 days
    # Beat scheduler for periodic tasks
    beat_schedule={
        'cleanup-stale-jobs': {
            'task': 'tasks.cleanup_stale_jobs',
            'schedule': 300.0,  # Run every 5 minutes
        },
    },
)


# ============== Helper Functions ==============

def which_any(candidates):
    """Find the first available executable from a list of candidates"""
    for name in candidates:
        p = shutil.which(name)
        if p:
            return p
    return None


def run_cmd(cmd, cwd=None, job_id=None):
    """Run a command and capture output for debugging"""
    cmd_str = ' '.join(str(c) for c in cmd)
    log_msg = f"[CMD] {cmd_str}"
    if job_id:
        append_job_log(job_id, log_msg)
    else:
        logger.info(log_msg)
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            cwd=cwd,
            capture_output=True,
            text=True
        )
        if result.stdout:
            stdout_msg = f"[STDOUT] {result.stdout[:500]}"
            if job_id:
                append_job_log(job_id, stdout_msg)
        return result
    except subprocess.CalledProcessError as e:
        error_msg = f"[CMD FAILED] Command failed with code {e.returncode}\n"
        error_msg += f"STDOUT: {(e.stdout or 'No stdout')[:500]}\n"
        error_msg += f"STDERR: {(e.stderr or 'No stderr')[:500]}"
        
        if job_id:
            append_job_log(job_id, error_msg)
        
        raise RuntimeError(f"Command failed: {cmd_str}\n{error_msg}") from e


def filter_contigs(src_fa, dst_fa, min_len, job_id=None):
    """Filter contigs by minimum length"""
    from Bio import SeqIO
    
    log_msg = f"Filtering contigs from {src_fa} (min_len={min_len})"
    if job_id:
        append_job_log(job_id, log_msg)
    
    if min_len <= 0:
        with open(src_fa, "rb") as r, open(dst_fa, "wb") as w:
            w.write(r.read())
        return
    
    records = (r for r in SeqIO.parse(src_fa, "fasta") if len(r.seq) >= min_len)
    n = SeqIO.write(records, dst_fa, "fasta")
    
    result_msg = f"Contigs >= {min_len} bp: {n}"
    if job_id:
        append_job_log(job_id, result_msg)


# ============== Assembly Functions ==============

def perform_illumina_assembly(r1_path: str, r2_path: str, output_dir: Path, 
                               threads: int, memory: int, min_contig: int, 
                               assembler: str = None, job_id: str = None):
    """Perform Illumina assembly using SPAdes"""
    append_job_log(job_id, "Starting Illumina assembly with SPAdes")
    
    spades = assembler if assembler else which_any(["spades.py", "spades"])
    if not spades:
        raise RuntimeError("SPAdes not found in PATH.")
    
    append_job_log(job_id, f"Using SPAdes at: {spades}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    cmd = [spades, "--threads", str(threads), "--memory", str(memory), "-o", str(output_dir)]
    
    if r2_path:
        cmd += ["-1", r1_path, "-2", r2_path]
        append_job_log(job_id, f"Paired-end mode: R1={r1_path}, R2={r2_path}")
    elif r1_path:
        cmd += ["-s", r1_path]
        append_job_log(job_id, f"Single-end mode: {r1_path}")
    else:
        raise ValueError("For Illumina specify --r1 (and optionally --r2).")
    
    run_cmd(cmd, job_id=job_id)
    
    # Update heartbeat after long-running command
    if job_id:
        update_heartbeat(job_id)
    
    contigs = output_dir / "contigs.fasta"
    if not contigs.exists():
        raise RuntimeError("SPAdes did not generate contigs.fasta")
    
    append_job_log(job_id, f"SPAdes completed. Contigs file: {contigs}")
    
    final = output_dir / "contigs.filtered.fasta"
    filter_contigs(contigs, final, min_contig, job_id=job_id)
    append_job_log(job_id, f"Illumina assembly completed: {final}")
    return final


def perform_ont_assembly(reads_path: str, output_dir: Path, threads: int, 
                          min_contig: int, assembler: str = None, job_id: str = None):
    """Perform ONT assembly using Flye or Raven"""
    append_job_log(job_id, "Starting ONT assembly")
    append_job_log(job_id, f"Input reads: {reads_path}")
    append_job_log(job_id, f"Threads: {threads}, Min contig: {min_contig}")
    
    if not os.path.exists(reads_path):
        raise RuntimeError(f"Reads file not found: {reads_path}")
    
    file_size = os.path.getsize(reads_path)
    append_job_log(job_id, f"Reads file size: {file_size / (1024*1024):.2f} MB")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    exe = None
    mode = None
    if assembler in (None, "flye"):
        exe = which_any(["flye"])
        mode = "flye" if exe else None
    if not exe and assembler in (None, "raven"):
        exe = which_any(["raven"])
        mode = "raven" if exe else None
    
    if not exe:
        raise RuntimeError("Flye/Raven not found in PATH.")
    
    append_job_log(job_id, f"Using assembler: {mode} ({exe})")
    
    if mode == "flye":
        cmd = ["flye", "--nano-raw", reads_path, "--out-dir", str(output_dir), "--threads", str(threads)]
        run_cmd(cmd, job_id=job_id)
        # Update heartbeat after long-running command
        if job_id:
            update_heartbeat(job_id)
        contigs = output_dir / "assembly.fasta"
    else:
        contigs = output_dir / "contigs.fasta"
        cmd = ["raven", "-t", str(threads), reads_path]
        append_job_log(job_id, f"Running Raven, output to: {contigs}")
        try:
            with open(contigs, "wb") as w:
                result = subprocess.run(cmd, check=True, stdout=w, stderr=subprocess.PIPE, text=True)
                if result.stderr:
                    append_job_log(job_id, f"[RAVEN STDERR] {result.stderr[:500]}")
            # Update heartbeat after long-running command
            if job_id:
                update_heartbeat(job_id)
        except subprocess.CalledProcessError as e:
            error_msg = f"Raven failed with code {e.returncode}\nSTDERR: {e.stderr or 'No stderr'}"
            append_job_log(job_id, f"[ERROR] {error_msg}")
            raise RuntimeError(error_msg) from e
    
    if not contigs.exists():
        raise RuntimeError(f"Contigs not found after ONT assembly: {contigs}")
    
    append_job_log(job_id, f"Assembly completed. Contigs: {contigs}")
    
    final = output_dir / "contigs.filtered.fasta"
    filter_contigs(contigs, final, min_contig, job_id=job_id)
    append_job_log(job_id, f"ONT assembly completed: {final}")
    return final


def perform_bakta_annotation(contigs_path: str, output_dir: Path, threads: int, job_id: str = None):
    """Perform genome annotation using Bakta"""
    append_job_log(job_id, "Starting genome annotation with Bakta")
    
    bakta = which_any(["bakta"])
    if not bakta:
        raise RuntimeError("Bakta not found in PATH.")
    
    append_job_log(job_id, f"Using Bakta at: {bakta}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        bakta,
        "--db", "/data/bakta_db",
        "--threads", str(threads),
        "--output", str(output_dir),
        "--force",
        contigs_path
    ]
    
    run_cmd(cmd, job_id=job_id)
    
    # Update heartbeat after long-running command
    if job_id:
        update_heartbeat(job_id)
    
    base = Path(contigs_path).stem
    preferred_gff3 = output_dir / f"{base}.gff3"
    if preferred_gff3.exists():
        gff_file = preferred_gff3
    else:
        gff_candidates = list(output_dir.glob("*.gff3"))
        if not gff_candidates:
            files = [p.name for p in output_dir.glob("*")]
            raise RuntimeError(f"Bakta did not generate a .gff3 file. Files: {files}")
        gff_file = gff_candidates[0]
        append_job_log(job_id, f"GFF3 basename mismatch; using: {gff_file.name}")
    
    append_job_log(job_id, f"Bakta annotation completed. GFF3: {gff_file}")
    return gff_file


# ============== Celery Tasks ==============

@celery_app.task(
    bind=True,
    name="tasks.run_assembly",
    soft_time_limit=ASSEMBLY_SOFT_TIMEOUT,
    time_limit=ASSEMBLY_HARD_TIMEOUT
)
def run_assembly_task(self, job_id: str, tech: str, files: dict, params: dict):
    """
    Celery task to run genome assembly.
    
    Concurrency is managed by Celery's worker settings (--concurrency flag).
    Timeouts: soft={ASSEMBLY_SOFT_TIMEOUT}s, hard={ASSEMBLY_HARD_TIMEOUT}s
    """
    job_dir = WORKDIR / job_id
    
    try:
        # Check if this is a retry after worker crash (job was left in 'running' state)
        r = get_redis()
        current_status = r.hget(f"job:{job_id}", "status")
        if current_status == "running":
            append_job_log(job_id, "Detected stale 'running' status - previous worker may have crashed. Restarting...")
        
        append_job_log(job_id, f"Starting {tech} assembly job (Celery task)")
        append_job_log(job_id, f"Parameters: {params}")
        update_job(job_id, status="running", stage="assembly", celery_task_id=self.request.id)
        update_heartbeat(job_id)  # Initial heartbeat
        
        if tech == "illumina":
            result = perform_illumina_assembly(
                r1_path=files["r1"],
                r2_path=files.get("r2"),
                output_dir=job_dir,
                threads=params["threads"],
                memory=params["memory"],
                min_contig=params["min_contig"],
                assembler=params.get("assembler"),
                job_id=job_id
            )
        else:  # ont
            result = perform_ont_assembly(
                reads_path=files["reads"],
                output_dir=job_dir,
                threads=params["threads"],
                min_contig=params["min_contig"],
                assembler=params.get("assembler"),
                job_id=job_id
            )
        
        append_job_log(job_id, f"Assembly completed successfully. Result: {result}")
        update_job(
            job_id,
            status="assembled",
            stage="assembly",
            result_file=str(result),
            message="Assembly completed successfully"
        )
        
        # Auto-annotate if requested
        if params.get("auto_annotate"):
            ann_threads = int(params.get("annotation_threads") or 4)
            append_job_log(job_id, f"Auto-annotation requested. Queuing Bakta with threads={ann_threads}")
            update_job(job_id, status="annotation_pending", stage="annotation", message="Annotation queued")
            # Chain to annotation task
            run_annotation_task.delay(job_id, str(result), ann_threads)
        
        return {"job_id": job_id, "status": "assembled", "result_file": str(result)}
    
    except SoftTimeLimitExceeded:
        error_msg = f"Assembly timed out after {ASSEMBLY_SOFT_TIMEOUT} seconds"
        append_job_log(job_id, f"[TIMEOUT] {error_msg}")
        update_job(job_id, status="failed", stage="assembly", message=error_msg)
        return {"job_id": job_id, "status": "failed", "error": error_msg}
        
    except Exception as e:
        error_msg = f"Assembly failed: {str(e)}"
        error_trace = traceback.format_exc()
        append_job_log(job_id, f"[ERROR] {error_msg}")
        append_job_log(job_id, f"[TRACEBACK] {error_trace}")
        update_job(job_id, status="failed", stage="assembly", message=error_msg, error_trace=error_trace)
        return {"job_id": job_id, "status": "failed", "error": error_msg}


@celery_app.task(
    bind=True,
    name="tasks.run_annotation",
    soft_time_limit=ANNOTATION_SOFT_TIMEOUT,
    time_limit=ANNOTATION_HARD_TIMEOUT
)
def run_annotation_task(self, job_id: str, contigs_path: str, threads: int):
    """
    Celery task to run genome annotation with Bakta.
    
    Concurrency is managed by Celery's worker settings (--concurrency flag).
    Timeouts: soft={ANNOTATION_SOFT_TIMEOUT}s, hard={ANNOTATION_HARD_TIMEOUT}s
    """
    job_dir = WORKDIR / job_id / "annotation"
    
    try:
        # Check if this is a retry after worker crash (job was left in 'running' state)
        r = get_redis()
        current_status = r.hget(f"job:{job_id}", "status")
        if current_status == "running":
            append_job_log(job_id, "Detected stale 'running' status - previous worker may have crashed. Restarting...")
        
        append_job_log(job_id, "Starting annotation job (Celery task)")
        update_job(job_id, status="running", stage="annotation", celery_task_id=self.request.id)
        update_heartbeat(job_id)  # Initial heartbeat
        
        gff_file = perform_bakta_annotation(
            contigs_path=contigs_path,
            output_dir=job_dir,
            threads=threads,
            job_id=job_id
        )
        
        append_job_log(job_id, f"Annotation completed successfully. GFF: {gff_file}")
        update_job(
            job_id,
            status="annotated",
            stage="annotation",
            annotation_file=str(gff_file),
            message="Annotation completed successfully"
        )
        
        return {"job_id": job_id, "status": "annotated", "annotation_file": str(gff_file)}
    
    except SoftTimeLimitExceeded:
        error_msg = f"Annotation timed out after {ANNOTATION_SOFT_TIMEOUT} seconds"
        append_job_log(job_id, f"[TIMEOUT] {error_msg}")
        update_job(job_id, status="failed", stage="annotation", message=error_msg)
        return {"job_id": job_id, "status": "failed", "error": error_msg}
        
    except Exception as e:
        error_msg = f"Annotation failed: {str(e)}"
        error_trace = traceback.format_exc()
        append_job_log(job_id, f"[ERROR] {error_msg}")
        append_job_log(job_id, f"[TRACEBACK] {error_trace}")
        update_job(job_id, status="failed", stage="annotation", message=error_msg, error_trace=error_trace)
        return {"job_id": job_id, "status": "failed", "error": error_msg}


@celery_app.task(name="tasks.cleanup_stale_jobs")
def cleanup_stale_jobs():
    """
    Periodic task to detect and mark stale jobs as failed.
    
    A job is considered stale if:
    - It has status 'running'
    - Its last heartbeat was more than HEARTBEAT_TIMEOUT seconds ago
    
    This handles cases where workers crash without updating job status.
    Runs every 5 minutes via Celery Beat.
    """
    job_ids = get_all_jobs()
    stale_count = 0
    
    for job_id in job_ids:
        job = get_job(job_id)
        if job and is_job_stale(job):
            stale_count += 1
            stage = job.get("stage", "unknown")
            error_msg = f"Job marked as failed: no heartbeat for {HEARTBEAT_TIMEOUT} seconds (worker may have crashed)"
            append_job_log(job_id, f"[STALE] {error_msg}")
            update_job(
                job_id,
                status="failed",
                stage=stage,
                message=error_msg
            )
            logger.warning(f"Marked stale job {job_id} as failed")
    
    if stale_count > 0:
        logger.info(f"Cleanup task: marked {stale_count} stale job(s) as failed")
    
    return {"stale_jobs_cleaned": stale_count}
