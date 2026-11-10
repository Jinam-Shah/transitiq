"""GTFS-Realtime collector: snapshot MBTA feeds on an interval, write compact bronze Parquet.

    transitiq-realtime collect --interval 30                  # run until stopped (Docker service)
    transitiq-realtime collect --interval 30 --minutes 60     # one hour
    transitiq-realtime replay tests/fixtures/gtfs_rt          # raw .pb(.gz) files -> bronze

Every snapshot is parsed (feeds.py) and buffered; every `flush_minutes` the buffer is written as
one Parquet file per feed and UTC day:
    source=gtfs_rt_vehicle_positions/ingest_date=D/vp_<first>_<last>.parquet
    source=gtfs_rt_trip_updates/ingest_date=D/tu_<first>_<last>.parquet
A raw protobuf snapshot is also kept every `raw_every_minutes` (gzip) so any day can be replayed:
    source=gtfs_rt_raw/ingest_date=D/<feed>_<ts>.pb.gz
Snapshots whose feed header timestamp did not change since the previous one are skipped, so a
stale feed is never double-counted. Bronze keys are unique and never overwritten.

Gaps in collection are logged with their cause (D-024): "suspended" means the wall clock jumped
while the monotonic clock did not, i.e. the PC slept or Docker paused its VM; "slow" means the
loop itself took too long (slow or hanging fetches). Both are counted in the stats line.
"""

from __future__ import annotations

import gzip
import io
import logging
import signal
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from transitiq_pipelines.http import HttpClient, RetryPolicy
from transitiq_pipelines.realtime import feeds
from transitiq_pipelines.sources import version_tag
from transitiq_pipelines.storage import BronzeExists, BronzeStore

log = logging.getLogger(__name__)

FEEDS = {
    "vp": (
        "gtfs_rt_vehicle_positions",
        feeds.VP_URL,
        feeds.parse_vehicle_positions,
        feeds.VP_SCHEMA,
    ),
    "tu": ("gtfs_rt_trip_updates", feeds.TU_URL, feeds.parse_trip_updates, feeds.TU_SCHEMA),
}


class Collector:
    def __init__(
        self,
        store: BronzeStore,
        http: HttpClient | None = None,
        flush_minutes: int = 10,
        raw_every_minutes: int = 15,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.store = store
        self.http = http or HttpClient(
            policy=RetryPolicy(max_attempts=3, base_delay=2, max_delay=20), timeout=20.0
        )
        self.flush_every = timedelta(minutes=flush_minutes)
        self.raw_every = timedelta(minutes=raw_every_minutes)
        self.clock = clock
        self.buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)  # (feed, date) -> rows
        self.snapshots: dict[tuple[str, str], list[datetime]] = defaultdict(list)
        self.last_feed_ts: dict[str, datetime] = {}
        self.last_raw: dict[str, datetime] = {}
        self.last_flush = clock()
        self.stats = {
            "snapshots": 0,
            "skipped_stale": 0,
            "errors": 0,
            "files": 0,
            "rows": 0,
            "slow_loops": 0,
            "suspended_seconds": 0,
            "max_fetch_seconds": 0.0,
        }

    # ---------- one snapshot ----------
    def ingest_bytes(self, feed: str, data: bytes, keep_raw: bool | None = None) -> int:
        """Parse one protobuf snapshot into the buffer. Returns rows added (0 if stale)."""
        _, _, parse, _ = FEEDS[feed]
        feed_ts, rows = parse(data)
        if feed_ts is None:
            raise ValueError(f"{feed}: feed header has no timestamp")
        if self.last_feed_ts.get(feed) == feed_ts:
            self.stats["skipped_stale"] += 1
            return 0
        self.last_feed_ts[feed] = feed_ts
        day = feed_ts.date().isoformat()
        self.buffers[(feed, day)].extend(rows)
        self.snapshots[(feed, day)].append(feed_ts)
        self.stats["snapshots"] += 1
        due = feed not in self.last_raw or feed_ts - self.last_raw[feed] >= self.raw_every
        if keep_raw if keep_raw is not None else due:
            self._put_raw(feed, feed_ts, data)
            self.last_raw[feed] = feed_ts
        return len(rows)

    def _put_raw(self, feed: str, feed_ts: datetime, data: bytes) -> None:
        key = f"source=gtfs_rt_raw/ingest_date={feed_ts.date()}/{feed}_{version_tag(feed_ts)}.pb.gz"
        meta = {
            "source": "gtfs_rt_raw",
            "feed": feed,
            "feed_ts": feed_ts.isoformat(),
            "bytes": len(data),
            "rows": None,
        }
        try:
            self.store.put_bytes(key, gzip.compress(data), meta)
        except BronzeExists:
            pass

    def snapshot(self) -> None:
        for feed, (_, url, _, _) in FEEDS.items():
            started = time.monotonic()
            try:
                self.ingest_bytes(feed, self.http.get(url).body)
            except Exception as exc:  # one bad fetch must not stop the collector
                self.stats["errors"] += 1
                log.warning("snapshot %s failed: %s", feed, exc)
            took = round(time.monotonic() - started, 1)
            self.stats["max_fetch_seconds"] = max(self.stats["max_fetch_seconds"], took)
            if took > 20:
                log.warning("fetching %s took %.0f s", feed, took)

    # ---------- flush ----------
    def flush(self) -> list[str]:
        written = []
        for (feed, day), rows in sorted(self.buffers.items()):
            if not rows:
                continue
            source, _, _, schema = FEEDS[feed]
            stamps = self.snapshots[(feed, day)]
            first, last = min(stamps), max(stamps)
            key = (
                f"source={source}/ingest_date={day}/"
                f"{feed}_{version_tag(first)}_{version_tag(last)}.parquet"
            )
            buf = io.BytesIO()
            pq.write_table(feeds.to_table(rows, schema), buf, compression="zstd")
            meta = {
                "source": source,
                "rows": len(rows),
                "snapshots": len(stamps),
                "first_feed_ts": first.isoformat(),
                "last_feed_ts": last.isoformat(),
                "fetched_at": self.clock().isoformat(),
                "bytes": buf.tell(),
            }
            try:
                self.store.put_bytes(key, buf.getvalue(), meta)
                written.append(key)
                self.stats["files"] += 1
                self.stats["rows"] += len(rows)
            except BronzeExists:
                log.info("already stored: %s", key)
        self.buffers.clear()
        self.snapshots.clear()
        self.last_flush = self.clock()
        return written

    # ---------- loops ----------
    def run(self, interval_seconds: int = 30, minutes: float | None = None) -> dict:
        stop = {"now": False}

        def _stop(*_):
            stop["now"] = True

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        end = self.clock() + timedelta(minutes=minutes) if minutes else None
        try:
            prev_wall, prev_mono = time.time(), time.monotonic()
            while not stop["now"] and (end is None or self.clock() < end):
                started = time.monotonic()
                self.note_gap(time.time() - prev_wall, started - prev_mono, interval_seconds)
                prev_wall, prev_mono = time.time(), started
                self.snapshot()
                if self.clock() - self.last_flush >= self.flush_every:
                    self.flush()
                    log.info("collector stats %s", self.stats)
                time.sleep(max(0.0, interval_seconds - (time.monotonic() - started)))
        finally:
            self.flush()  # never lose the buffer on stop
        return self.stats

    def note_gap(
        self, wall_seconds: float, mono_seconds: float, interval_seconds: int
    ) -> str | None:
        """Classify the time since the previous loop; log and count anything unusual."""
        cause = gap_cause(wall_seconds, mono_seconds, interval_seconds)
        if cause == "suspended":
            self.stats["suspended_seconds"] += int(wall_seconds - mono_seconds)
            log.warning(
                "collection gap of %.0f s: host or Docker was suspended (PC asleep?)", wall_seconds
            )
        elif cause == "slow":
            self.stats["slow_loops"] += 1
            log.warning("collection gap of %.0f s: the previous loop was slow", wall_seconds)
        return cause


def gap_cause(wall_seconds: float, mono_seconds: float, interval_seconds: int) -> str | None:
    """None for a normal loop, "suspended" if the machine slept, "slow" if the loop overran.

    Linux's monotonic clock stops while the machine (or Docker Desktop's VM) is suspended, but
    the wall clock is corrected on resume, so a large difference between them means a sleep.
    """
    if wall_seconds - mono_seconds > 60:
        return "suspended"
    if mono_seconds > 3 * interval_seconds:
        return "slow"
    return None


def replay(collector: Collector, paths: list[Path]) -> dict:
    """Feed recorded raw snapshots (vp_*.pb[.gz] / tu_*.pb[.gz]) through the same code path."""
    for path in sorted(paths):
        name = path.name
        feed = "vp" if name.startswith("vp_") else "tu" if name.startswith("tu_") else None
        if feed is None:
            continue
        data = path.read_bytes()
        if name.endswith(".gz"):
            data = gzip.decompress(data)
        collector.ingest_bytes(feed, data, keep_raw=False)
    collector.flush()
    return collector.stats
