"""Settings, read from environment variables (and the repo's .env file when run locally)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from transitiq_pipelines import __version__

USER_AGENT = f"TransitIQ/{__version__} (+https://github.com/Jinam-Shah/transitiq)"


def load_dotenv(path: Path) -> None:
    """Minimal .env loader: KEY=VALUE lines, # comments. Never overrides existing variables."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(frozen=True)
class Paths:
    data_dir: Path

    @property
    def bronze(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def checkpoints(self) -> Path:
        return self.data_dir / "checkpoints"

    @property
    def runs(self) -> Path:
        return self.data_dir / "runs"

    @property
    def tmp(self) -> Path:
        return self.data_dir / "tmp"

    def ensure(self) -> Paths:
        for p in (self.bronze, self.checkpoints, self.runs, self.tmp):
            p.mkdir(parents=True, exist_ok=True)
        return self


def mbta_api_key() -> str | None:
    return os.getenv("MBTA_API_KEY") or None
