"""Iceberg warehouse (PyIceberg + SQLite catalog) and the quarantine area.

    data/warehouse/catalog.db        SQLite Iceberg catalog
    data/warehouse/silver.db/<table> Iceberg table data + metadata (Parquet)
    data/quarantine/<table>/<partition>.parquet   rejected rows with _reason

Writes are idempotent: partitioned tables overwrite exactly the partitions being loaded,
other tables are replaced whole. Re-loading the same input never duplicates rows.

Runs on Linux (Docker or CI). PyIceberg cannot yet handle Windows drive-letter paths
(apache/iceberg-python#3720), so the CLI refuses to run on Windows; see D-012.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, In

from transitiq_pipelines.fsutil import atomic_replace
from transitiq_pipelines.silver.contracts import ContractError, same_schema

NAMESPACE = "silver"

# Overwriting a partition that does not exist yet is normal for us (first load of a day).
warnings.filterwarnings("ignore", message="Delete operation did not match any records")


class Warehouse:
    def __init__(self, data_dir: Path):
        self.root = Path(data_dir).resolve() / "warehouse"
        self.quarantine_root = Path(data_dir).resolve() / "quarantine"
        self.root.mkdir(parents=True, exist_ok=True)
        self.catalog = SqlCatalog(
            "transitiq",
            uri=f"sqlite:///{(self.root / 'catalog.db').as_posix()}",
            warehouse=self.root.as_uri(),
        )
        self.catalog.create_namespace_if_not_exists(NAMESPACE)

    # ---------- tables ----------
    def table(self, name: str, contract: pa.Schema, partition_by: str | None = None):
        identifier = f"{NAMESPACE}.{name}"
        try:
            table = self.catalog.load_table(identifier)
        except NoSuchTableError:
            table = self.catalog.create_table(identifier, schema=contract)
            if partition_by:
                with table.update_spec() as spec:
                    spec.add_identity(partition_by)
            return table
        diffs = same_schema(table.schema().as_arrow(), contract)
        if diffs:
            raise ContractError(
                f"Iceberg table {identifier} no longer matches its contract: {diffs}. "
                "If the contract change is intended, rebuild with --rebuild."
            )
        return table

    def drop(self, name: str) -> None:
        try:
            self.catalog.drop_table(f"{NAMESPACE}.{name}")
        except NoSuchTableError:
            pass

    def replace_all(self, name: str, contract: pa.Schema, data: pa.Table) -> None:
        self.table(name, contract).overwrite(data.cast(contract), overwrite_filter=AlwaysTrue())

    def replace_partitions(
        self, name: str, contract: pa.Schema, column: str, values: list[str], data: pa.Table
    ) -> None:
        table = self.table(name, contract, partition_by=column)
        table.overwrite(data.cast(contract), overwrite_filter=In(column, set(values)))

    def record_count(self, name: str) -> int:
        try:
            snapshot = self.catalog.load_table(f"{NAMESPACE}.{name}").current_snapshot()
        except NoSuchTableError:
            return 0
        return int(snapshot.summary["total-records"]) if snapshot else 0

    def read(self, name: str, row_filter=None) -> pa.Table:
        table = self.catalog.load_table(f"{NAMESPACE}.{name}")
        return (
            table.scan(row_filter=row_filter).to_arrow() if row_filter else table.scan().to_arrow()
        )

    def data_files(self, name: str) -> list[str]:
        """Parquet files of the current snapshot, as local paths.

        Our writes are copy-on-write (overwrite rewrites whole files), so the current snapshot
        has no delete files and reading exactly these files gives exactly the table. We check
        that instead of assuming it.
        """
        try:
            table = self.catalog.load_table(f"{NAMESPACE}.{name}")
        except NoSuchTableError:
            return []
        files = []
        for task in table.scan().plan_files():
            if task.delete_files:
                raise ContractError(f"{name} has delete files; publish needs copy-on-write tables")
            path = task.file.file_path
            files.append(path.removeprefix("file://"))
        return sorted(files)

    # ---------- quarantine ----------
    def write_quarantine(self, name: str, partition: str, rejected: pa.Table) -> Path | None:
        """Write (or clear) one partition's quarantine file. Deterministic path = idempotent."""
        path = self.quarantine_root / name / f"{partition}.parquet"
        if rejected.num_rows == 0:
            path.unlink(missing_ok=True)
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        pq.write_table(rejected, tmp)
        atomic_replace(tmp, path)
        return path

    def clear_quarantine(self, name: str) -> None:
        folder = self.quarantine_root / name
        if folder.exists():
            for f in folder.glob("*.parquet"):
                f.unlink()
