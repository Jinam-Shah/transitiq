# Airflow 3.1 plus an ISOLATED virtualenv for TransitIQ's own dependencies (D-017).
# Airflow's Python stays untouched (no dependency conflicts with dbt or PyIceberg); tasks call
# /opt/transitiq-venv/bin/python with the repo (mounted at /app) on PYTHONPATH,
# so code changes need no rebuild. Only dependency changes do.
FROM apache/airflow:3.1.0-python3.12

# The image runs as user "airflow" (uid 50000, group 0), which cannot write to /opt.
USER root
RUN mkdir -p /opt/transitiq-venv && chown airflow:0 /opt/transitiq-venv \
    && apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY pipelines/requirements-runtime.txt /tmp/requirements-runtime.txt
# The base image sets PIP_USER=true (installs into ~/.local), which pip refuses inside a
# virtualenv, so it is switched off for this install only.
RUN python -m venv /opt/transitiq-venv \
    && PIP_USER=false /opt/transitiq-venv/bin/pip install --no-cache-dir \
        -r /tmp/requirements-runtime.txt \
    && /opt/transitiq-venv/bin/python -c "import duckdb, pyiceberg, psycopg, dbt.version, lightgbm, mlflow"
