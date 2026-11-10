"""Publish compact gold tables from DuckDB to PostgreSQL (local Docker Postgres and/or Neon).

Only what the app needs goes to Postgres; the 13.7M-row fact tables stay in DuckDB (Neon's free
plan has 1 GB per project; D-018). Each table is published atomically:

    BEGIN
      CREATE TABLE gold.<t>__new (...typed...)    -- types mapped from DuckDB
      COPY gold.<t>__new FROM STDIN               -- rows streamed from DuckDB
      DROP TABLE IF EXISTS gold.<t>; ALTER TABLE gold.<t>__new RENAME TO <t>
      ALTER TABLE gold.<t> ADD PRIMARY KEY (...)
      INSERT INTO gold.publish_log ...
    COMMIT

so readers see either the old table or the new one, never a half-loaded table, and re-running
is idempotent. Row limits per table are checked before anything is written, and the database
size is checked afterwards against a budget.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

PG_TYPES = {
    "VARCHAR": "text",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "DOUBLE": "double precision",
    "DATE": "date",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "BOOLEAN": "boolean",
}


@dataclass(frozen=True)
class PublishSpec:
    table: str  # schema.table in DuckDB; published as gold.<name> in Postgres
    primary_key: tuple[str, ...] | None
    max_rows: int
    where: str = ""  # optional filter, relative to the table's own newest data
    optional: bool = False  # skip silently if the DuckDB table does not exist yet

    @property
    def name(self) -> str:
        return self.table.split(".", 1)[1]


PUBLISH = (
    PublishSpec("gold.dim_route", ("route_id",), 5_000),
    PublishSpec("gold.dim_stop", ("stop_id",), 50_000),
    PublishSpec("gold.dim_date", ("date",), 10_000),
    PublishSpec("gold.dim_time_bucket", ("bucket",), 96),
    PublishSpec("gold.dim_weather_hour", ("hour_ts",), 100_000),
    PublishSpec(
        "gold.mart_reliability_route_hour", ("route_id", "day_type", "time_bucket"), 200_000
    ),
    PublishSpec("gold.mart_stop_reliability", ("stop_id", "day_type", "time_bucket"), 1_000_000),
    PublishSpec("gold.fact_facility_outage", ("alert_id", "facility_id"), 200_000),
    PublishSpec(
        "gold.fact_alert",
        ("alert_id", "last_modified_ts"),
        1_000_000,
        "last_modified_ts >= "
        "(select max(last_modified_ts) from gold.fact_alert) - interval 180 day",
    ),
    PublishSpec(
        "gold.fact_bunching_event",
        None,
        1_000_000,
        "ts >= (select max(ts) from gold.fact_bunching_event) - interval 90 day",
    ),
    PublishSpec(
        "gold.fact_ghost_trip",
        ("service_date", "trip_id"),
        1_000_000,
        "service_date >= (select max(service_date) from gold.fact_ghost_trip) - interval 35 day",
    ),
    PublishSpec(
        "ops.quality_results",
        None,
        100_000,
        "run_at >= (select max(run_at) from ops.quality_results) - interval 30 day",
        optional=True,
    ),
)

DEFAULT_BUDGET_BYTES = 800 * 1024 * 1024  # stay well under Neon's 1 GB free limit


class PublishError(Exception):
    pass


def local_url() -> str:
    """Docker-network Postgres from docker-compose (override with TRANSITIQ_PG_URL)."""
    if os.getenv("TRANSITIQ_PG_URL"):
        return os.environ["TRANSITIQ_PG_URL"]
    user = os.getenv("POSTGRES_USER", "transitiq")
    password = os.getenv("POSTGRES_PASSWORD", "transitiq_local_only")
    db = os.getenv("POSTGRES_DB", "transitiq")
    return f"postgresql://{user}:{password}@postgres:5432/{db}"


def targets(choice: str) -> list[tuple[str, str]]:
    neon = os.getenv("NEON_DATABASE_URL", "").strip()
    if choice == "local":
        return [("local", local_url())]
    if choice == "neon":
        if not neon:
            raise PublishError("NEON_DATABASE_URL is not set")
        return [("neon", neon)]
    return [("local", local_url())] + ([("neon", neon)] if neon else [])  # auto


def _exists(duck, spec: PublishSpec) -> bool:
    schema, table = spec.table.split(".", 1)
    return bool(
        duck.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = ? and table_name = ?",
            [schema, table],
        ).fetchone()[0]
    )


def _columns(duck, spec: PublishSpec) -> list[tuple[str, str]]:
    schema, table = spec.table.split(".", 1)
    rows = duck.execute(
        "select column_name, data_type from information_schema.columns "
        "where table_schema = ? and table_name = ? order by ordinal_position",
        [schema, table],
    ).fetchall()
    if not rows:
        raise PublishError(f"{spec.table} does not exist in DuckDB (run dbt build first)")
    unknown = [t for _, t in rows if t not in PG_TYPES]
    if unknown:
        raise PublishError(f"{spec.table}: no Postgres type for {unknown}")
    return rows


def _select(spec: PublishSpec) -> str:
    where = f" where {spec.where}" if spec.where else ""
    return f"select * from {spec.table}{where}"  # noqa: S608 (specs are code constants)


def plan(duck, specs=PUBLISH) -> list[tuple[PublishSpec, list[tuple[str, str]], int]]:
    """Validate every table before writing anything: exists, types known, under row limit."""
    out = []
    for spec in specs:
        if spec.optional and not _exists(duck, spec):
            continue
        cols = _columns(duck, spec)
        n = duck.execute(f"select count(*) from ({_select(spec)})").fetchone()[0]  # noqa: S608
        if n > spec.max_rows:
            raise PublishError(f"{spec.table}: {n:,} rows exceeds limit {spec.max_rows:,}")
        out.append((spec, cols, n))
    return out


def publish(db_path: Path, url: str, budget_bytes: int = DEFAULT_BUDGET_BYTES, specs=PUBLISH):
    import psycopg  # lazy: not needed for the rest of the package

    duck = duckdb.connect(str(db_path), read_only=True)
    results = []
    try:
        planned = plan(duck, specs)
        with psycopg.connect(url, connect_timeout=30) as pg:
            with pg.cursor() as cur:
                cur.execute("create schema if not exists gold")
                cur.execute(
                    "create table if not exists gold.publish_log (table_name text, rows bigint, "
                    "published_at timestamptz, source text)"
                )
            pg.commit()
            for spec, cols, n in planned:
                new = f"gold.{spec.name}__new"
                ddl = ", ".join(f'"{c}" {PG_TYPES[t]}' for c, t in cols)
                names = ", ".join(f'"{c}"' for c, _ in cols)
                with pg.cursor() as cur:
                    cur.execute(f"drop table if exists {new}")
                    cur.execute(f"create table {new} ({ddl})")
                    reader = duck.execute(_select(spec)).fetch_record_batch(50_000)
                    with cur.copy(f"copy {new} ({names}) from stdin") as copy:
                        for batch in reader:
                            for row in zip(
                                *(col.to_pylist() for col in batch.columns), strict=True
                            ):
                                copy.write_row(row)
                    cur.execute(f"drop table if exists gold.{spec.name}")
                    cur.execute(f"alter table {new} rename to {spec.name}")
                    if spec.primary_key:
                        pk = ", ".join(f'"{c}"' for c in spec.primary_key)
                        cur.execute(f"alter table gold.{spec.name} add primary key ({pk})")
                    cur.execute(
                        "insert into gold.publish_log values (%s, %s, %s, %s)",
                        (spec.name, n, datetime.now(UTC), str(db_path)),
                    )
                pg.commit()  # one transaction per table: atomic swap
                results.append((spec.name, n))
            with pg.cursor() as cur:
                cur.execute("select pg_database_size(current_database())")
                size = cur.fetchone()[0]
    finally:
        duck.close()
    if size > budget_bytes:
        raise PublishError(
            f"database is {size / 1e6:.0f} MB, over the {budget_bytes / 1e6:.0f} MB budget"
        )
    return results, size
