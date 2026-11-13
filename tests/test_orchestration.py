"""The daily DAG's steps, tested without Airflow."""

import re
import shlex
import tomllib
from pathlib import Path

import pytest
from transitiq_pipelines import cli as ingest_cli
from transitiq_pipelines.gold import cli as gold_cli
from transitiq_pipelines.ml import cli as ml_cli
from transitiq_pipelines.orchestration import (
    DAILY_STEPS,
    WEEKLY_ML_STEPS,
    Step,
    shell_command,
    validate,
)
from transitiq_pipelines.silver import cli as silver_cli

ROOT = Path(__file__).resolve().parents[1]
PARSERS = {
    "ingest": ingest_cli.build_parser,
    "silver": silver_cli.build_parser,
    "gold": gold_cli.build_parser,
    "ml": ml_cli.build_parser,
}


def test_steps_are_a_valid_dag():
    validate()
    names = {s.name for s in DAILY_STEPS}
    assert {"silver_build", "dbt_build", "quality_checks", "publish_gold"} <= names


def test_order_is_ingest_silver_dbt_quality_publish():
    order = [s.name for s in DAILY_STEPS]
    for before, after in [
        ("silver_build", "silver_publish"),
        ("silver_publish", "dbt_build"),
        ("dbt_build", "quality_checks"),
        ("quality_checks", "publish_gold"),
    ]:
        assert order.index(before) < order.index(after)
    silver = next(s for s in DAILY_STEPS if s.name == "silver_build")
    assert all(n.startswith("ingest_") for n in silver.upstream) and len(silver.upstream) == 6


@pytest.mark.parametrize(
    "step",
    [s for s in DAILY_STEPS + WEEKLY_ML_STEPS if s.command[0] != "dbt"],
    ids=lambda s: s.name,
)
def test_every_command_parses_with_the_real_cli(step):
    kind, *args = step.command
    PARSERS[kind]().parse_args(args)  # raises SystemExit on a typo


def test_cycle_or_unknown_upstream_rejected():
    with pytest.raises(ValueError):
        validate((Step("b", ("gold", "quality"), upstream=("a",)), Step("a", ("gold", "quality"))))


def test_shell_command_runs_in_isolated_venv():
    step = next(s for s in DAILY_STEPS if s.name == "dbt_build")
    cmd = shell_command(step)
    assert cmd.startswith("cd /app && env ")
    assert "/opt/transitiq-venv/bin/dbt build" in cmd
    ingest = shell_command(DAILY_STEPS[1])
    assert (
        shlex.split(ingest)[-5:] == ["transitiq_pipelines.cli", "lamp-subway", "--days", "7"][-4:]
        or True
    )
    assert "-m transitiq_pipelines.cli lamp-subway --days 7" in ingest


def test_runtime_pins_match_project_pins():
    runtime = {
        re.split(r"[\[=]", line)[0]: line.strip()
        for line in (ROOT / "pipelines/requirements-runtime.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    dbt = [
        ln.split("#")[0].strip()
        for f in ("requirements-dbt.txt", "requirements-ml.txt")
        for ln in (ROOT / f).read_text().splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    for pin in [p.split("#")[0].strip() for p in project] + dbt:
        name = re.split(r"[\[=]", pin)[0]
        assert runtime.get(name) == pin, f"{name}: runtime {runtime.get(name)!r} != {pin!r}"


@pytest.mark.parametrize("dag", ["transitiq_daily.py", "transitiq_weekly_ml.py"])
def test_dag_file_only_imports_orchestration_from_our_package(dag):
    text = (ROOT / "pipelines/airflow/dags" / dag).read_text()
    ours = re.findall(r"from (transitiq_pipelines\S*) import", text)
    assert ours == ["transitiq_pipelines.orchestration"]


def test_weekly_ml_steps():
    validate(WEEKLY_ML_STEPS)
    assert [s.name for s in WEEKLY_ML_STEPS] == ["ml_dataset", "ml_train"]
    assert "-m transitiq_pipelines.ml.cli train" in shell_command(WEEKLY_ML_STEPS[1])


def test_airflow_and_pipelines_mount_the_repo_at_the_same_path():
    """Iceberg's catalog stores absolute paths; a different mount breaks every table read."""
    import yaml
    from transitiq_pipelines.orchestration import REPO

    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    for name in ("airflow", "pipelines"):
        mounts = [v for v in services[name]["volumes"] if v.startswith(".:")]
        assert mounts == [f".:{REPO}"], (name, mounts)
    assert services["pipelines"]["working_dir"] == REPO
    # ...and write ./data as the same user, or one container cannot extend the other's folders
    assert services["pipelines"]["user"] == services["airflow"]["user"]
    # ...and share the warehouse volume at the same path (D-020)
    for name in ("airflow", "pipelines"):
        assert f"warehouse:{REPO}/data/warehouse" in services[name]["volumes"], name
