# syntax=docker/dockerfile:1

# Stage 1: Builder
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# Install curl for healthchecks in dev mode
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Enable bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install dependencies first (better caching)
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev --extra k8s

# Copy application code
COPY osa/ ./osa/
COPY sources/ ./sources/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/
COPY alembic.ini ./
COPY entrypoint.sh ./
COPY README.md ./
# Install the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra k8s

# Stage 2: Runtime
FROM python:3.13-slim-bookworm AS runtime

ARG OSA_VERSION=dev
ARG OSA_REVISION=unknown
LABEL org.opencontainers.image.title="OSA Server"
LABEL org.opencontainers.image.description="Open Science Archive reference implementation"
LABEL org.opencontainers.image.source="https://github.com/opensciencearchive/server"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.version="${OSA_VERSION}"
LABEL org.opencontainers.image.revision="${OSA_REVISION}"

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv

COPY --from=builder --chown=appuser:appuser /app/osa /app/osa
COPY --from=builder --chown=appuser:appuser /app/sources /app/sources
COPY --from=builder --chown=appuser:appuser /app/migrations /app/migrations
COPY --from=builder --chown=appuser:appuser /app/scripts /app/scripts
COPY --from=builder --chown=appuser:appuser /app/alembic.ini /app/alembic.ini

# Ensure the virtualenv is used
ENV PATH="/app/.venv/bin:$PATH"

# Create data directory
RUN mkdir -p /data && chown appuser:appuser /data
ENV OSA_DATA_DIR=/data

COPY --from=builder --chown=appuser:appuser /app/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER appuser

EXPOSE 8000

# Health check
HEALTHCHECK --interval=5s --timeout=10s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8000/api/v1/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["uvicorn", "--factory", "osa.application.api.rest.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
