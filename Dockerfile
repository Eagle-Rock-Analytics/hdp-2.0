# HDP 2.0 pipeline image — single image, four pipeline entrypoints.
#
# Build:   docker build -t hdp:latest .
# Smoke:   docker run --rm hdp:latest smoke
# Run:     docker run --rm hdp:latest run-qaqc --station=ASOSAWOS_72290993115 --append
#
# Entrypoints (see docker/entrypoint.sh): run-pull | run-clean | run-qaqc | run-merge.
# Used by AWS Batch job definitions in Phase 2 (hdp-gh5.4); the command selects
# the stage and args are passed straight through to the underlying script.

FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

# System libraries required by cartopy / geopandas (GEOS, PROJ, GDAL).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgeos-dev \
        libproj-dev \
        libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency layer first so it is cached unless the lockfile changes.
# --no-install-project: the scripts run by path (flat imports via PYTHONPATH),
# not as an installed package, so only third-party deps are needed.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Application code.
COPY scripts/ ./scripts/
COPY data/ ./data/
COPY docker/entrypoint.sh docker/smoke_test.sh ./docker/
RUN chmod +x ./docker/entrypoint.sh ./docker/smoke_test.sh

# Non-root runtime user. Own /app so runtime log dirs (qaqc_logs/, merge_logs/)
# created relative to the working directory are writable.
RUN useradd --create-home --uid 10001 hdp \
    && chown -R hdp:hdp /app
USER hdp

# Put the uv-managed venv on PATH and make scripts/ importable (paths.py etc.).
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app/scripts" \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["smoke"]
