"""Per-source checkpoints so backfills resume where they stopped.

Stored as data/checkpoints/<source>.json:
    {"source": "...", "updated_at": "...", "partitions": {"2026-11-04": {...}, ...}}

Saved atomically (write temp file, then rename) after every partition, so a crash or Ctrl+C
loses at most the partition in flight. Thread-safe for parallel downloads.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path

from transitiq_pipelines.fsutil import atomic_replace


class Checkpoint:
    def __init__(self, directory: Path, source: str):
        self.path = Path(directory) / f"{source}.json"
        self.source = source
        self._lock = threading.Lock()
        if self.path.exists():
            self.data = json.loads(self.path.read_text("utf-8"))
        else:
            self.data = {"source": source, "partitions": {}}

    def get(self, partition: str) -> dict | None:
        with self._lock:
            return self.data["partitions"].get(partition)

    def mark(self, partition: str, **info) -> None:
        with self._lock:
            self.data["partitions"][partition] = info
            self.data["updated_at"] = datetime.now(UTC).isoformat()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(self.path.name + ".tmp")
            tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), "utf-8")
            atomic_replace(tmp, self.path)

    def partitions(self) -> dict[str, dict]:
        with self._lock:
            return dict(self.data["partitions"])
