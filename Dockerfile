# NEXUS application image: API, scheduler, worker and migrations all run
# from this image with different commands (see docker-compose.yml). The same
# file is the build for the Vercel Daemons (deploy/vercel.daemons.json).
#
# PYTHON_IMAGE can point at an internal registry mirror of python:3.12-slim.
ARG PYTHON_IMAGE=python:3.12-slim

FROM ${PYTHON_IMAGE} AS builder
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
RUN pip install "uv==0.11.32"
WORKDIR /app
# Dependencies first (cached until the lockfile changes), then the project.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime
RUN useradd --system --uid 10001 --user-group --home-dir /app --no-create-home nexus
WORKDIR /app
COPY --from=builder --chown=nexus:nexus /app/.venv /app/.venv
COPY --chown=nexus:nexus alembic.ini ./
COPY --chown=nexus:nexus migrations ./migrations
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER nexus
EXPOSE 8000
# Exec form: the process is PID 1 and receives SIGTERM directly, which the
# API (uvicorn) and the daemons (graceful drain) both handle.
CMD ["uvicorn", "nexus.main:app", "--host", "0.0.0.0", "--port", "8000"]
