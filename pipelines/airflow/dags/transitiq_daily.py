"""Daily TransitIQ refresh: ingest -> silver -> dbt gold -> quality checks -> publish.

Steps are defined in transitiq_pipelines.orchestration (unit-tested without Airflow); this file
only turns them into BashOperator tasks. Each task runs one idempotent CLI command inside the
isolated /opt/transitiq-venv of the custom Airflow image (D-017).
"""

from datetime import datetime, timedelta

from transitiq_pipelines.orchestration import DAILY_STEPS, shell_command, validate

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

validate()

with DAG(
    dag_id="transitiq_daily",
    description="Ingest, rebuild silver and gold, check quality, publish to Postgres",
    schedule="17 10 * * *",  # 10:17 UTC (05:17/06:17 Boston), after LAMP publishes yesterday
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retry_delay": timedelta(minutes=5), "owner": "transitiq"},
    tags=["transitiq", "daily"],
    doc_md=__doc__,
) as dag:
    tasks = {
        step.name: BashOperator(
            task_id=step.name,
            bash_command=shell_command(step),
            retries=step.retries,
            execution_timeout=timedelta(minutes=step.timeout_minutes),
            doc_md=step.doc or None,
        )
        for step in DAILY_STEPS
    }
    for step in DAILY_STEPS:
        for upstream in step.upstream:
            tasks[upstream] >> tasks[step.name]
