"""Filesystem helpers.

On Windows, os.replace() fails with PermissionError (WinError 5 "Access is denied") when
another process briefly holds the target open, typically antivirus (Windows Defender) or the
search indexer scanning a file that was just written. The lock clears within milliseconds,
so we retry with a short backoff instead of failing the partition.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path


def atomic_replace(
    src: Path,
    dest: Path,
    attempts: int = 10,
    delay: float = 0.05,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    for attempt in range(1, attempts + 1):
        try:
            os.replace(src, dest)
            return
        except PermissionError:
            if attempt == attempts:
                raise
            sleep(delay * attempt)  # 50 ms, 100 ms, ... about 2.75 s in total
