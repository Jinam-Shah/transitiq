"""The daily and weekly pipelines as plain data, so it can be tested without Airflow.

pipelines/airflow/dags/transitiq_daily.py turns DAILY_STEPS into Airflow tasks. Keep this module
stdlib-only: Airflow's own Python imports it while parsing the DAG.

Every step is an idempotent CLI call (re-running any step or the whole DAG is safe), executed
in the isolated transitiq virtualenv inside the Airflow image (D-017).
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field

# Same mount path as the `pipelines` service: Iceberg's catalog stores absolute file paths,
# so every container that reads the warehouse must see the repo at the same place (D-017).
REPO = "/app"
VENV = "/opt/transitiq-venv"


@dataclass(frozen=True)
class Step:
    name: str
    command: tuple[str, ...]  # argv; the first item is "ingest", "silver", "gold" or "dbt"
    upstream: tuple[str, ...] = ()
    retries: int = 0
    timeout_minutes: int = 60
    doc: str = ""
    extra_env: dict = field(default_factory=dict)


MODULES = {
    "ingest": "transitiq_pipelines.cli",
    "silver": "transitiq_pipelines.silver.cli",
    "gold": "transitiq_pipelines.gold.cli",
    "ml": "transitiq_pipelines.ml.cli",
}

INGEST = (
    "ingest_gtfs",
    "ingest_lamp_subway",
    "ingest_weather",
    "ingest_lamp_alerts",
    "ingest_v3_alerts",
    "ingest_v3_facilities",
)

DAILY_STEPS: tuple[Step, ...] = (
    Step(
        "ingest_gtfs",
        ("ingest", "gtfs-static"),
        retries=2,
        timeout_minutes=15,
        doc="Current GTFS feed; skipped when the ETag is unchanged.",
    ),
    Step(
        "ingest_lamp_subway",
        ("ingest", "lamp-subway", "--days", "7"),
        retries=2,
        doc="Last 7 service days, so late or republished LAMP files are picked up.",
    ),
    Step(
        "ingest_weather",
        ("ingest", "weather", "--days", "14"),
        retries=2,
        timeout_minutes=15,
        doc="Last 14 days of Open-Meteo history (the archive lags by several days).",
    ),
    Step(
        "ingest_lamp_alerts",
        ("ingest", "lamp-alerts"),
        retries=2,
        doc="Alerts history file; downloaded at most weekly or when it changes.",
    ),
    Step("ingest_v3_alerts", ("ingest", "v3", "alerts"), retries=2, timeout_minutes=10),
    Step("ingest_v3_facilities", ("ingest", "v3", "facilities"), retries=2, timeout_minutes=10),
    Step(
        "silver_build",
        ("silver", "build"),
        upstream=INGEST,
        timeout_minutes=90,
        doc="Validate changed bronze partitions into Iceberg; unchanged ones are skipped.",
    ),
    Step("silver_publish", ("silver", "publish"), upstream=("silver_build",), timeout_minutes=10),
    Step(
        "dbt_build",
        ("dbt", "build"),
        upstream=("silver_publish",),
        retries=1,
        timeout_minutes=60,
        doc="Gold models, enforced contracts and dbt tests.",
    ),
    Step("quality_checks", ("gold", "quality"), upstream=("dbt_build",), timeout_minutes=15),
    Step(
        "publish_gold",
        ("gold", "publish", "--target", "auto"),
        upstream=("quality_checks",),
        retries=2,
        timeout_minutes=30,
        doc="Compact gold tables to local Postgres, and to Neon when NEON_DATABASE_URL is set.",
    ),
)


# Weekly retraining of the Leave-By Planner models (D-027). Runs after the Sunday daily refresh;
# it reads gold from DuckDB, so it must not overlap a dbt build (max_active_runs and a later
# start time; both DAGs use the same file only for reading here).
WEEKLY_ML_STEPS: tuple[Step, ...] = (
    Step(
        "ml_dataset",
        ("ml", "dataset"),
        timeout_minutes=60,
        doc="Simulated riders from gold; only new months (and the current one) are rebuilt.",
    ),
    Step(
        "ml_train",
        ("ml", "train"),
        upstream=("ml_dataset",),
        timeout_minutes=90,
        doc="Fit p50/p80/p90/p95 models, calibrate on recent days, test on the newest 60 days.",
    ),
)


def argv(step: Step, venv: str = VENV) -> list[str]:
    kind, *args = step.command
    if kind == "dbt":
        return [f"{venv}/bin/dbt", *args]
    return [f"{venv}/bin/python", "-m", MODULES[kind], *args]


def shell_command(step: Step, repo: str = REPO, venv: str = VENV) -> str:
    env = {
        "PYTHONPATH": f"{repo}/pipelines/src",
        "DBT_PROJECT_DIR": "dbt",
        "DBT_PROFILES_DIR": "dbt",
        **step.extra_env,
    }
    exports = " ".join(f"{k}={shlex.quote(v)}" for k, v in env.items())
    return f"cd {shlex.quote(repo)} && env {exports} {shlex.join(argv(step, venv))}"


def validate(steps: tuple[Step, ...] = DAILY_STEPS) -> None:
    names = [s.name for s in steps]
    if len(names) != len(set(names)):
        raise ValueError("duplicate step names")
    known: set[str] = set()
    for s in steps:  # steps must be listed after their upstreams (also proves no cycles)
        missing = [u for u in s.upstream if u not in known]
        if missing:
            raise ValueError(f"{s.name}: upstream {missing} not defined before it")
        known.add(s.name)
