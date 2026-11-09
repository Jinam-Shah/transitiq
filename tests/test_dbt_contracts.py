"""Contract test: dbt's view of silver must match the silver contracts exactly.

dbt/models/staging/sources.yml declares every silver column and type. If a silver contract
changes (or someone edits the YAML), this fails before dbt models break at runtime.
"""

from pathlib import Path

import yaml
from transitiq_pipelines.silver.contracts import SILVER_TABLES

ROOT = Path(__file__).resolve().parents[1]
DUCKDB = {
    "string": "varchar",
    "int32": "integer",
    "int64": "bigint",
    "double": "double",
    "date32[day]": "date",
    "timestamp[us, tz=UTC]": "timestamptz",
}


def dbt_sources() -> dict[str, list[tuple[str, str]]]:
    doc = yaml.safe_load((ROOT / "dbt/models/staging/sources.yml").read_text("utf-8"))
    (silver,) = [s for s in doc["sources"] if s["name"] == "silver"]
    return {
        t["name"]: [(c["name"], c["data_type"]) for c in t["columns"]] for t in silver["tables"]
    }


def test_every_silver_table_is_a_dbt_source():
    assert set(dbt_sources()) == set(SILVER_TABLES)


def test_columns_and_types_match_contracts():
    sources = dbt_sources()
    for name, schema in SILVER_TABLES.items():
        expected = [(f.name, DUCKDB[str(f.type)]) for f in schema]
        assert sources[name] == expected, name


def test_gold_models_declare_contracts():
    project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text("utf-8"))
    assert project["models"]["transitiq"]["marts"]["+contract"]["enforced"] is True
    for yml in (ROOT / "dbt/models/marts").rglob("*.yml"):
        for model in yaml.safe_load(yml.read_text("utf-8"))["models"]:
            assert model["columns"], model["name"]
            assert all("data_type" in c for c in model["columns"]), model["name"]
