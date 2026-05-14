FROM ghcr.io/astral-sh/uv:python3.13-alpine3.23

WORKDIR /app/

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_DEV=0 \
    PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml pyproject.toml
RUN uv sync

COPY saga_orchestrator/ ./saga_orchestrator/
COPY tests/ ./tests
CMD ["pytest"]
