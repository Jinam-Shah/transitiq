"""MBTA GTFS static schedule: https://cdn.mbta.com/MBTA_GTFS.zip (~20 MB).

Checked with a HEAD request; downloaded only when the ETag changes, so daily runs are cheap.
    source=gtfs_static/fetched_date=YYYY-MM-DD/MBTA_GTFS_<etag12>.zip
The sidecar records feed_version and row counts for every .txt file in the zip.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from pathlib import Path

from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.sources import Context
from transitiq_pipelines.storage import BronzeExists

SOURCE = "gtfs_static"
URL = "https://cdn.mbta.com/MBTA_GTFS.zip"


def etag_tag(etag: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]", "", etag)
    return cleaned[:12] or "noetag"


def zip_summary(path: Path) -> tuple[dict[str, int], str | None]:
    """Row counts per GTFS file (excluding headers) and the feed_version, if present."""
    counts: dict[str, int] = {}
    feed_version = None
    with zipfile.ZipFile(path) as zf:
        for name in sorted(zf.namelist()):
            if not name.endswith(".txt"):
                continue
            with zf.open(name) as fh:
                reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8-sig", newline=""))
                header = next(reader, None)
                rows = 0
                for row in reader:
                    rows += 1
                    if name == "feed_info.txt" and header and "feed_version" in header:
                        feed_version = row[header.index("feed_version")]
                counts[name] = rows
    return counts, feed_version


def run(ctx: Context, url: str = URL, force: bool = False) -> RunStats:
    stats = RunStats(SOURCE)
    ckpt = ctx.checkpoint(SOURCE)
    try:
        etag = ctx.http.head(url).get("etag", "")
        prev = ckpt.get("current")
        if (
            not force
            and prev
            and etag
            and prev.get("etag") == etag
            and ctx.store.exists(prev["key"])
        ):
            stats.add_skipped()
            return stats.finish()
        tmp = ctx.tmp_dir / f"{SOURCE}.zip"
        try:
            dl = ctx.http.download(url, tmp)
            etag = dl["headers"].get("etag", etag)
            counts, feed_version = zip_summary(tmp)
            rows = sum(counts.values())
            key = f"source={SOURCE}/fetched_date={ctx.today}/MBTA_GTFS_{etag_tag(etag)}.zip"
            meta = {
                "source": SOURCE,
                "source_url": url,
                "etag": etag,
                "feed_version": feed_version,
                "fetched_at": ctx.now.isoformat(),
                "sha256": dl["sha256"],
                "bytes": dl["bytes"],
                "rows": rows,
                "rows_by_file": counts,
            }
            try:
                ctx.store.put_file(key, tmp, meta)
                stats.add_fetched(rows, dl["bytes"])
            except BronzeExists:
                stats.add_skipped()
            ckpt.mark("current", key=key, etag=etag, feed_version=feed_version, rows=rows)
        finally:
            tmp.unlink(missing_ok=True)
    except Exception as exc:
        stats.add_failure("current", exc)
    return stats.finish()
