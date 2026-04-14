# =============================================================================
# Production-ready Airflow image with e-commerce pipeline dependencies
# =============================================================================
# Base: Official Airflow 2.9.3 with Python 3.12
FROM apache/airflow:2.9.3-python3.12

# Git version metadata (passed via build args)
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG VERSION=unknown
ARG BUILD_DATE

# Add OCI labels for image metadata
LABEL org.opencontainers.image.title="ecom-datalake-pipeline"
LABEL org.opencontainers.image.description="E-commerce data lakehouse pipeline with dbt, DuckDB, and BigQuery"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${GIT_COMMIT}"
LABEL org.opencontainers.image.source="https://github.com/G-Schumacher44/ecom_datalake_pipelines"
LABEL com.ecom.git.branch="${GIT_BRANCH}"
LABEL com.ecom.git.commit="${GIT_COMMIT}"
LABEL com.ecom.version="${VERSION}"

# Switch to root for system package installation
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Google Cloud SDK (for bq, gsutil)
    curl \
    gnupg \
    lsb-release \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y google-cloud-sdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
ENV PATH="/home/airflow/.local/bin:${PATH}"
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy dependency files first (for layer caching)
COPY --chown=airflow:root pyproject.toml constraints.txt ./
COPY --chown=airflow:root src/ ./src/
COPY --chown=airflow:root config/ ./config/
COPY --chown=airflow:root airflow/dags/ ./dags/
COPY --chown=airflow:root samples/ ./samples/
COPY --chown=airflow:root scripts/ ./scripts/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    # Install core dependencies with constraints for deterministic builds
    pip install --no-cache-dir -c constraints.txt -e ".[airflow,dbt]" && \
    dbt --version

# Copy application code
COPY --chown=airflow:root dbt_duckdb/ ./dbt_duckdb/
COPY --chown=airflow:root dbt_bigquery/ ./dbt_bigquery/

# Samples are mounted at runtime via docker-compose (not baked into image)
# This avoids macOS Docker build context timeout issues with many small files

# Install dbt dependencies (dbt-utils, etc.)
RUN cd dbt_duckdb && dbt deps --project-dir . --profiles-dir . && \
    cd ../dbt_bigquery && dbt deps --project-dir . --profiles-dir .

# Switch to root to create global symlinks (fixes PATH/Entrypoint issues)
USER root
RUN ln -s /home/airflow/.local/bin/airflow /usr/local/bin/airflow && \
    ln -s /home/airflow/.local/bin/dbt /usr/local/bin/dbt

# Switch back to airflow user for safety
USER airflow

# Create necessary directories for local runs
RUN mkdir -p \
    data/silver/base \
    data/silver/enriched \
    data/metrics \
    data/logs \
    docs/validation_reports

# Health check: Verify Python package is importable
RUN python -c "from src.settings import load_settings; print('✅ Package installed successfully')"

# Set environment defaults (can be overridden in docker-compose or Cloud Composer)
ENV PIPELINE_ENV=local \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False \
    GIT_COMMIT=${GIT_COMMIT} \
    GIT_BRANCH=${GIT_BRANCH} \
    PIPELINE_VERSION=${VERSION}

# Default command (overridden by docker-compose)
CMD ["airflow", "webserver"]
