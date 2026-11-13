"""Weekly retraining of the Leave-By Planner journey-time models (D-027).

ml_dataset -> ml_train, every Sunday at 12:47 UTC, after the daily refresh (10:17 UTC) has
rebuilt gold. Steps are defined in transitiq_pipelines.orchestration, like the daily DAG.
"""

from datetime import datetime, timedelta

from transitiq_pipelines.orchestration import WEEKLY_ML_STEPS, shell_command, validate

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

validate(WEEKLY_ML_STEPS)

with DAG(
    dag_id="transitiq_weekly_ml",
    description="Rebuild the training rows and retrain the Leave-By Planner quantile models",
    schedule="47 12 * * 0",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retry_delay": timedelta(minutes=10), "owner": "transitiq"},
    tags=["transitiq", "ml", "weekly"],
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
        for step in WEEKLY_ML_STEPS
    }
    for step in WEEKLY_ML_STEPS:
        for upstream in step.upstream:
            tasks[upstream] >> tasks[step.name]
