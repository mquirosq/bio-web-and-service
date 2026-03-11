FROM continuumio/miniconda3:latest

WORKDIR /app

# Create conda environment and install all dependencies in one layer
RUN conda create -n bio_env python=3.11 -y && \
    conda install -n bio_env -c bioconda -c conda-forge \
    spades raven-assembler bakta biopython \
    fastapi uvicorn python-multipart gunicorn -y && \
    conda clean -afy

# Install Redis client and Celery via pip
RUN conda run -n bio_env pip install redis celery flower

# Make conda environment activate by default
SHELL ["conda", "run", "-n", "bio_env", "/bin/bash", "-c"]
ENV PATH=/opt/conda/envs/bio_env/bin:$PATH

# Create non-root user for security
RUN useradd -m -u 1000 biouser && \
    mkdir -p /data/bakta_db /data/workdir && \
    chown -R biouser:biouser /app /data

# Install system dependencies before switching to non-root user
RUN apt-get update && \
    apt-get install -y libopenmpi40 && \
    rm -rf /var/lib/apt/lists/*

# Copy application files
COPY --chown=biouser:biouser app.py tasks.py utils.py gunicorn.conf.py /app/

# Switch to non-root user
USER biouser

# Expose port
EXPOSE 8000