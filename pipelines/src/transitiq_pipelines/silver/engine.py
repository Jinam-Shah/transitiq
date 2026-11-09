"""Validate-and-split engine shared by every silver table.

A table builder supplies a DuckDB SQL query that returns the contract columns plus two
helper columns:
    _row_number   position of the row in its bronze file (to trace it back)
    _reason       NULL for a valid row, otherwise why it is rejected (e.g. "negative_duration")

split() runs it once, then returns the valid rows (cast to the contract, with lineage
columns) and the rejected rows (with reason and lineage) plus counts per reason.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

import duckdb
import pyarrow as pa

from transitiq_pipelines.silver.contracts import ContractError, same_schema


def connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'UTC'")
    conn.execute("SET enable_progress_bar = false")
    return conn


@dataclass
class SplitResult:
    valid: pa.Table
    rejected: pa.Table
    reasons: Counter = field(default_factory=Counter)

    @property
    def rows_in(self) -> int:
        return self.valid.num_rows + self.rejected.num_rows


def _with_lineage(table: pa.Table, source_file: str, sha256: str | None, loaded_at: datetime):
    n = table.num_rows
    return (
        table.append_column("_source_file", pa.array([source_file] * n, pa.string()))
        .append_column("_source_sha256", pa.array([sha256] * n, pa.string()))
        .append_column("_loaded_at", pa.array([loaded_at] * n, pa.timestamp("us", tz="UTC")))
    )


def split(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    contract: pa.Schema,
    source_file: str,
    sha256: str | None,
    loaded_at: datetime,
    params: list | None = None,
) -> SplitResult:
    conn.execute("DROP TABLE IF EXISTS _stage")
    conn.execute(f"CREATE TEMP TABLE _stage AS {sql}", params or [])  # noqa: S608 (internal SQL)

    data_cols = [f.name for f in contract if not f.name.startswith("_")]
    cols_sql = ", ".join(f'"{c}"' for c in data_cols)
    valid = conn.execute(
        f"SELECT {cols_sql} FROM _stage WHERE _reason IS NULL ORDER BY _row_number"  # noqa: S608
    ).to_arrow_table()
    rejected = conn.execute(
        f"SELECT _reason, _row_number, {cols_sql} FROM _stage "  # noqa: S608
        "WHERE _reason IS NOT NULL ORDER BY _row_number"
    ).to_arrow_table()
    reasons = Counter(
        dict(
            conn.execute(
                "SELECT _reason, count(*) FROM _stage WHERE _reason IS NOT NULL GROUP BY 1"
            ).fetchall()
        )
    )
    conn.execute("DROP TABLE _stage")

    valid = _with_lineage(valid, source_file, sha256, loaded_at)
    diffs = same_schema(valid.schema, contract)
    if diffs:
        raise ContractError(f"builder output does not match contract: {diffs}")
    valid = valid.cast(contract)
    rejected = _with_lineage(rejected, source_file, sha256, loaded_at)
    return SplitResult(valid=valid, rejected=rejected, reasons=reasons)


def concat(results: list[SplitResult], contract: pa.Schema) -> SplitResult:
    if not results:
        return SplitResult(contract.empty_table(), pa.table({}), Counter())
    reasons: Counter = Counter()
    for r in results:
        reasons.update(r.reasons)
    rejected = [r.rejected for r in results if r.rejected.num_rows]
    return SplitResult(
        valid=pa.concat_tables([r.valid for r in results]),
        rejected=pa.concat_tables(rejected, promote_options="permissive")
        if rejected
        else pa.table({}),
        reasons=reasons,
    )
