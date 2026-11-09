# Linux image for jobs that should not run on Windows directly (PyIceberg path bug, D-012):
# silver builds, dbt (gold), and model training in a later phase.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml requirements-dev.txt requirements-dbt.txt ./
COPY pipelines/src ./pipelines/src
RUN pip install -r requirements-dev.txt -r requirements-dbt.txt

# The repo is mounted at /app at runtime, so code edits apply without rebuilding.
CMD ["transitiq-silver", "--help"]
