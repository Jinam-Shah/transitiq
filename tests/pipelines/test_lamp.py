import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from conftest import Reply
from transitiq_pipelines.sources import lamp_alerts, lamp_subway


def parquet_bytes(rows: int) -> bytes:
    table = pa.table(
        {"stop_id": [f"s{i}" for i in range(rows)], "travel_time_seconds": list(range(rows))}
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def index_csv(base: str, entries: list[tuple[str, str]]) -> bytes:
    lines = ["size_bytes,last_modified,service_date,file_url"]
    for service_date, last_modified in entries:
        url = f"{base}/lamp/{service_date}-subway-on-time-performance-v1.parquet"
        lines.append(f"100,{last_modified},{service_date},{url}")
    return ("\n".join(lines) + "\n").encode()


def setup_lamp(fake_server, entries, rows=5):
    fake_server.add("/index.csv", Reply(200, index_csv(fake_server.base_url, entries)))
    for service_date, _ in entries:
        path = f"/lamp/{service_date}-subway-on-time-performance-v1.parquet"
        fake_server.add(path, Reply(200, parquet_bytes(rows)))


def run_lamp(ctx, fake_server, start="2026-11-01", end="2026-11-30"):
    return lamp_subway.run(
        ctx,
        date.fromisoformat(start),
        date.fromisoformat(end),
        workers=2,
        index_url=fake_server.base_url + "/index.csv",
        allowed_hosts=("127.0.0.1",),
    )


def test_parse_and_select():
    text = index_csv(
        "https://performancedata.mbta.com",
        [
            ("2026-10-31", "2026-11-01 00:00:00+00:00"),
            ("2026-11-01", "2026-11-02 00:00:00+00:00"),
            ("2026-11-03", "2026-11-04 01:02:03+00:00"),
        ],
    ).decode()
    chosen = lamp_subway.select(
        lamp_subway.parse_index(text), date(2026, 11, 1), date(2026, 11, 30)
    )
    assert [e.service_date.isoformat() for e in chosen] == ["2026-11-01", "2026-11-03"]
    assert lamp_subway.bronze_key(chosen[1]) == (
        "source=lamp_subway_otp/service_date=2026-11-03/"
        "2026-11-03-subway-on-time-performance-v1_v20261104T010203Z.parquet"
    )


def test_backfill_then_rerun_skips(ctx, fake_server):
    entries = [
        ("2026-11-01", "2026-11-02 00:00:00+00:00"),
        ("2026-11-02", "2026-11-03 00:00:00+00:00"),
    ]
    setup_lamp(fake_server, entries, rows=7)

    first = run_lamp(ctx, fake_server)
    assert (first.fetched, first.skipped, first.rows) == (2, 0, 14)
    assert first.ok

    second = run_lamp(ctx, fake_server)
    assert (second.fetched, second.skipped) == (0, 2)
    assert len(fake_server.hits("/lamp/2026-11-01-subway-on-time-performance-v1.parquet")) == 1


def test_republished_day_is_stored_as_new_version(ctx, fake_server):
    setup_lamp(fake_server, [("2026-11-01", "2026-11-02 00:00:00+00:00")])
    run_lamp(ctx, fake_server)
    setup_lamp(fake_server, [("2026-11-01", "2026-11-09 00:00:00+00:00")])
    again = run_lamp(ctx, fake_server)
    assert again.fetched == 1
    keys = [m["key"] for m in ctx.store.iter_meta()]
    assert len(keys) == 2  # both versions kept: bronze is immutable


def test_one_bad_day_does_not_stop_backfill(ctx, fake_server):
    entries = [
        ("2026-11-01", "2026-11-02 00:00:00+00:00"),
        ("2026-11-02", "2026-11-03 00:00:00+00:00"),
    ]
    setup_lamp(fake_server, entries)
    fake_server.add("/lamp/2026-11-01-subway-on-time-performance-v1.parquet", Reply(500))
    stats = run_lamp(ctx, fake_server)
    assert stats.fetched == 1
    assert [f["partition"] for f in stats.failures] == ["2026-11-01"]
    # the failed day is retried on the next run
    fake_server.add(
        "/lamp/2026-11-01-subway-on-time-performance-v1.parquet", Reply(200, parquet_bytes(3))
    )
    retry = run_lamp(ctx, fake_server)
    assert (retry.fetched, retry.skipped) == (1, 1)


def test_unexpected_host_rejected(ctx, fake_server):
    setup_lamp(fake_server, [("2026-11-01", "2026-11-02 00:00:00+00:00")])
    stats = lamp_subway.run(
        ctx,
        date(2026, 11, 1),
        date(2026, 11, 30),
        index_url=fake_server.base_url + "/index.csv",
        allowed_hosts=("performancedata.mbta.com",),
    )
    assert stats.fetched == 0 and "unexpected host" in stats.failures[0]["error"]


def test_lineage_metadata(ctx, fake_server):
    setup_lamp(fake_server, [("2026-11-01", "2026-11-02 00:00:00+00:00")], rows=4)
    run_lamp(ctx, fake_server)
    meta = next(ctx.store.iter_meta())
    assert meta["rows"] == 4
    assert meta["service_date"] == "2026-11-01"
    assert meta["source_url"].endswith("2026-11-01-subway-on-time-performance-v1.parquet")
    assert len(meta["sha256"]) == 64


def test_alerts_skip_when_unchanged(ctx, fake_server):
    lm = {"Last-Modified": "Fri, 06 Nov 2026 21:26:50 GMT"}
    fake_server.add("/alerts.parquet", Reply(200, parquet_bytes(9), lm))
    url = fake_server.base_url + "/alerts.parquet"

    first = lamp_alerts.run(ctx, url=url)
    assert (first.fetched, first.rows) == (1, 9)
    second = lamp_alerts.run(ctx, url=url)
    assert (second.fetched, second.skipped) == (0, 1)
    assert len(fake_server.hits("/alerts.parquet", "GET")) == 1
