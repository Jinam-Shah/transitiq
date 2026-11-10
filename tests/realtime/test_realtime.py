"""GTFS-Realtime parsing and collection, on snapshots recorded from the live MBTA feeds.

tests/fixtures/gtfs_rt holds real VehiclePositions / TripUpdates snapshots (2026-11-09, ~30 s
apart), cut down to a few routes by scripts/make_rt_fixture.py.
"""

import gzip
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from transitiq_pipelines.realtime import feeds
from transitiq_pipelines.realtime.collector import Collector, replay
from transitiq_pipelines.storage import LocalBronzeStore

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gtfs_rt"
VP_FILES = sorted(FIXTURES.glob("vp_*.pb.gz"))
TU_FILES = sorted(FIXTURES.glob("tu_*.pb.gz"))
windows_skip = pytest.mark.skipif(sys.platform == "win32", reason="Iceberg on Windows (D-012)")


def raw(path: Path) -> bytes:
    return gzip.decompress(path.read_bytes())


def test_fixtures_are_real_recordings():
    assert len(VP_FILES) >= 10 and len(TU_FILES) >= 10


def test_parse_vehicle_positions():
    feed_ts, rows = feeds.parse_vehicle_positions(raw(VP_FILES[0]))
    assert feed_ts.tzinfo is UTC and feed_ts.year == 2026
    assert rows and all(r["vehicle_id"] for r in rows)
    bus = [r for r in rows if r["schedule_relationship"] == "SCHEDULED"]
    assert bus and all(r["start_date"] and len(r["start_date"]) == 8 for r in bus)
    assert all(41 < r["latitude"] < 43.5 for r in rows)
    assert feeds.to_table(rows, feeds.VP_SCHEMA).schema == feeds.VP_SCHEMA


def test_parse_trip_updates_keeps_only_next_stop():
    feed_ts, rows = feeds.parse_trip_updates(raw(TU_FILES[0]))
    assert rows and len({r["trip_id"] for r in rows}) == len(rows)  # one row per trip
    scheduled = [r for r in rows if r["schedule_relationship"] == "SCHEDULED" and r["stop_updates"]]
    assert scheduled and all(r["next_stop_sequence"] is not None for r in scheduled)
    assert all(r["next_arrival_ts"] or r["next_departure_ts"] for r in scheduled)


def test_stale_snapshot_is_skipped(tmp_path):
    c = Collector(LocalBronzeStore(tmp_path))
    data = raw(VP_FILES[0])
    assert c.ingest_bytes("vp", data, keep_raw=False) > 0
    assert c.ingest_bytes("vp", data, keep_raw=False) == 0  # same header timestamp
    assert c.stats["skipped_stale"] == 1


def test_replay_writes_compact_bronze(tmp_path):
    store = LocalBronzeStore(tmp_path)
    stats = replay(Collector(store), VP_FILES + TU_FILES)
    assert stats["snapshots"] == len(VP_FILES) + len(TU_FILES) - stats["skipped_stale"]
    metas = list(store.iter_meta())
    sources = {m["source"] for m in metas}
    assert sources == {"gtfs_rt_vehicle_positions", "gtfs_rt_trip_updates"}
    for m in metas:
        table = pq.read_table(store.path(m["key"]))
        assert table.num_rows == m["rows"]
        assert m["snapshots"] >= 1 and m["first_feed_ts"] <= m["last_feed_ts"]


def test_raw_snapshots_kept_on_schedule(tmp_path):
    t0 = datetime(2026, 11, 9, tzinfo=UTC)
    c = Collector(LocalBronzeStore(tmp_path), raw_every_minutes=15, clock=lambda: t0)
    for path in VP_FILES[:3]:
        c.ingest_bytes("vp", raw(path))
    raws = [m for m in LocalBronzeStore(tmp_path).iter_meta() if m["source"] == "gtfs_rt_raw"]
    assert len(raws) == 1  # snapshots ~30 s apart: only the first is kept raw


@windows_skip
def test_silver_realtime_from_recorded_feeds(tmp_path):
    from transitiq_pipelines.silver.builders import SilverContext, build_realtime
    from transitiq_pipelines.silver.warehouse import Warehouse

    store = LocalBronzeStore(tmp_path / "bronze")
    replay(Collector(store), VP_FILES + TU_FILES)
    ctx = SilverContext(store=store, warehouse=Warehouse(tmp_path), checkpoint_dir=tmp_path / "ck")
    stats = {s.table: s for s in build_realtime(ctx)}
    assert all(s.ok for s in stats.values()), [s.failures for s in stats.values()]
    assert stats["rt_vehicle_positions"].rows_valid > 0
    assert stats["rt_trip_updates"].rows_valid > 0
    again = {s.table: s for s in build_realtime(ctx)}  # idempotent re-run
    assert all(s.partitions_loaded == 0 for s in again.values())


def test_gap_cause_tells_sleep_from_slow_loops(tmp_path):
    from transitiq_pipelines.realtime.collector import gap_cause

    assert gap_cause(30, 30, 30) is None
    assert gap_cause(70, 69, 30) is None  # a little slow, still fine
    assert gap_cause(3000, 31, 30) == "suspended"  # wall clock jumped, monotonic did not
    assert gap_cause(400, 400, 30) == "slow"
    c = Collector(LocalBronzeStore(tmp_path))
    c.note_gap(3000, 31, 30)
    c.note_gap(400, 400, 30)
    assert c.stats["suspended_seconds"] == 2969 and c.stats["slow_loops"] == 1
