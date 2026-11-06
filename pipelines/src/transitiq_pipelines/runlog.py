"""Run statistics: every ingestion run writes data/runs/<timestamp>_<source>.json.

These files are the evidence behind METRICS.md (files, rows, bytes, run time, failures).
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class RunStats:
    source: str
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at: str | None = None
    duration_seconds: float | None = None
    fetched: int = 0
    skipped: int = 0
    rows: int = 0
    bytes: int = 0
    failures: list[dict] = field(default_factory=list)
    _t0: float = field(default_factory=time.monotonic, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_fetched(self, rows: int, nbytes: int) -> None:
        with self._lock:
            self.fetched += 1
            self.rows += rows
            self.bytes += nbytes

    def add_skipped(self) -> None:
        with self._lock:
            self.skipped += 1

    def add_failure(self, partition: str, error: Exception | str) -> None:
        with self._lock:
            self.failures.append({"partition": partition, "error": str(error)[:500]})

    @property
    def ok(self) -> bool:
        return not self.failures

    def finish(self) -> RunStats:
        self.finished_at = datetime.now(UTC).isoformat()
        self.duration_seconds = round(time.monotonic() - self._t0, 2)
        return self

    def to_dict(self) -> dict:
        return {
            f.name: list(getattr(self, f.name)) if f.name == "failures" else getattr(self, f.name)
            for f in fields(self)
            if not f.name.startswith("_")
        }

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        path = directory / f"{stamp}_{self.source}.json"
        path.write_text(json.dumps(self.to_dict(), indent=2), "utf-8")
        return path
