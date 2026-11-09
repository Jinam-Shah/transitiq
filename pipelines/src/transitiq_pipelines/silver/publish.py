"""Publish silver Iceberg tables as DuckDB views for dbt.

    data/warehouse/transitiq.duckdb  schema "silver": one view per Iceberg table

Each view reads exactly the Parquet files of the table's current Iceberg snapshot (see
Warehouse.data_files). Re-run after every silver build; dbt then reads `silver.<table>`.
Empty or missing tables become empty views with the contract's columns, so dbt always compiles.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from transitiq_pipelines.silver.contracts import SILVER_TABLES

DUCKDB_TYPES = {
    pa.string(): "VARCHAR",
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
    pa.date32(): "DATE",
    pa.timestamp("us", tz="UTC"): "TIMESTAMPTZ",
}


def _quote(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"


def publish(warehouse, db_path: Path) -> dict[str, int]:
    """Create or replace silver.<table> views. Returns the number of files per table."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    counts = {}
    for name, contract in SILVER_TABLES.items():
        files = warehouse.data_files(name)
        if files:
            listing = ", ".join(_quote(f) for f in files)
            select = f"SELECT * FROM read_parquet([{listing}])"  # noqa: S608 (own file paths)
        else:
            cols = ", ".join(
                f'CAST(NULL AS {DUCKDB_TYPES[f.type]}) AS "{f.name}"' for f in contract
            )
            select = f"SELECT {cols} WHERE false"
        conn.execute(f"CREATE OR REPLACE VIEW silver.{name} AS {select}")  # noqa: S608
        counts[name] = len(files)
    conn.close()
    return counts
