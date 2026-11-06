"""MBTA LAMP subway on-time performance: one Parquet file per service date (2019-09-15 onward).

Index: https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv
    size_bytes,last_modified,service_date,file_url

Each row is one arrival/departure event with actual and scheduled travel time, dwell and
headway. This is the historical backbone for reliability metrics and model training.

LAMP sometimes republishes old dates (new last_modified). Bronze keeps every version:
    source=lamp_subway_otp/service_date=YYYY-MM-DD/<file>_v<last_modified>.parquet
A date is skipped when the checkpoint already holds the same last_modified.
"""

from __future__ import annotations

import csv
import io
import logging
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime

from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.sources import Context, parquet_rows, version_tag
from transitiq_pipelines.storage import BronzeExists

log = logging.getLogger(__name__)

SOURCE = "lamp_subway_otp"
INDEX_URL = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv"
ALLOWED_HOSTS = ("performancedata.mbta.com",)


@dataclass(frozen=True)
class IndexEntry:
    service_date: date
    file_url: str
    last_modified: datetime
    size_bytes: int


def parse_index(text: str) -> list[IndexEntry]:
    entries = []
    for row in csv.DictReader(io.StringIO(text)):
        entries.append(
            IndexEntry(
                service_date=date.fromisoformat(row["service_date"]),
                file_url=row["file_url"].strip(),
                last_modified=datetime.fromisoformat(row["last_modified"]),
                size_bytes=int(row["size_bytes"]),
            )
        )
    return entries


def select(entries: list[IndexEntry], start: date, end: date) -> list[IndexEntry]:
    return sorted(
        (e for e in entries if start <= e.service_date <= end), key=lambda e: e.service_date
    )


def bronze_key(entry: IndexEntry) -> str:
    name = urllib.parse.urlparse(entry.file_url).path.rsplit("/", 1)[-1]
    stem = name.removesuffix(".parquet")
    tag = version_tag(entry.last_modified)
    return f"source={SOURCE}/service_date={entry.service_date}/{stem}_v{tag}.parquet"


def _fetch_one(ctx: Context, entry: IndexEntry, stats: RunStats, ckpt, allowed_hosts) -> None:
    partition = entry.service_date.isoformat()
    lm = entry.last_modified.isoformat()
    prev = ckpt.get(partition)
    key = bronze_key(entry)
    if prev and prev.get("last_modified") == lm and ctx.store.exists(prev["key"]):
        stats.add_skipped()
        return
    host = urllib.parse.urlparse(entry.file_url).hostname
    if host not in allowed_hosts:
        stats.add_failure(partition, f"unexpected host {host!r} in index")
        return
    tmp = ctx.tmp_dir / f"{SOURCE}_{partition}.parquet"
    try:
        dl = ctx.http.download(entry.file_url, tmp)
        rows = parquet_rows(tmp)
        meta = {
            "source": SOURCE,
            "source_url": entry.file_url,
            "service_date": partition,
            "upstream_last_modified": lm,
            "fetched_at": ctx.now.isoformat(),
            "sha256": dl["sha256"],
            "bytes": dl["bytes"],
            "rows": rows,
        }
        try:
            ctx.store.put_file(key, tmp, meta)
            stats.add_fetched(rows, dl["bytes"])
        except BronzeExists:
            stats.add_skipped()  # same version already stored (e.g. checkpoint was deleted)
        ckpt.mark(partition, key=key, last_modified=lm, rows=rows, bytes=dl["bytes"])
    except Exception as exc:  # one bad day must not stop the backfill
        log.error("failed %s: %s", partition, exc)
        stats.add_failure(partition, exc)
    finally:
        tmp.unlink(missing_ok=True)


def run(
    ctx: Context,
    start: date,
    end: date,
    workers: int = 4,
    index_url: str = INDEX_URL,
    allowed_hosts: tuple[str, ...] = ALLOWED_HOSTS,
) -> RunStats:
    stats = RunStats(SOURCE)
    ckpt = ctx.checkpoint(SOURCE)
    try:
        index = parse_index(ctx.http.get(index_url).body.decode("utf-8"))
    except Exception as exc:
        stats.add_failure("index", exc)
        return stats.finish()
    wanted = select(index, start, end)
    log.info("%s: %d service dates in range %s..%s", SOURCE, len(wanted), start, end)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        list(pool.map(lambda e: _fetch_one(ctx, e, stats, ckpt, allowed_hosts), wanted))
    return stats.finish()
