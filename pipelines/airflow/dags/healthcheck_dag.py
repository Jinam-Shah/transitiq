"""Phase 0 smoke DAG: proves Airflow parses DAGs from this repo and can run a task."""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="transitiq_healthcheck",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transitiq", "phase-0"],
)
def transitiq_healthcheck():
    @task
    def say_hello() -> str:
        message = "TransitIQ Airflow is alive"
        print(message)
        return message

    say_hello()


transitiq_healthcheck()
