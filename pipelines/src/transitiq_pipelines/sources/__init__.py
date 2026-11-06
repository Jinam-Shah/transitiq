"""Source fetchers. Each module exposes run(ctx, ...) -> RunStats.

Modules are imported lazily by the CLI so the Lambda only loads what it uses.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from transitiq_pipelines.checkpoint import Checkpoint
from transitiq_pipelines.http import HttpClient
from transitiq_pipelines.storage import BronzeStore


@dataclass
class Context:
    store: BronzeStore
    http: HttpClient
    checkpoint_dir: Path
    tmp_dir: Path = field(default_factory=lambda: Path(tempfile.gettempdir()))
    now: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def today(self) -> date:
        return self.now.date()

    def checkpoint(self, source: str) -> Checkpoint:
        return Checkpoint(self.checkpoint_dir, source)


def version_tag(ts: datetime) -> str:
    """Filesystem-safe UTC timestamp used to version bronze files, e.g. 20261022T182647Z."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def parquet_rows(path: Path) -> int:
    import pyarrow.parquet as pq  # lazy: only local backfills need pyarrow

    return pq.read_metadata(path).num_rows
