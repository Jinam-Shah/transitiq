import io
import json
import zipfile
from datetime import date

from conftest import Reply
from transitiq_pipelines.sources import gtfs_static, mbta_v3, open_meteo


# ---------- Open-Meteo ----------
def test_month_chunks():
    chunks = open_meteo.month_chunks(date(2026, 1, 15), date(2026, 3, 10))
    assert [(m, s.isoformat(), e.isoformat()) for m, s, e, _ in chunks] == [
        ("2026-01", "2026-01-15", "2026-01-31"),
        ("2026-02", "2026-02-01", "2026-02-28"),
        ("2026-03", "2026-03-01", "2026-03-10"),
    ]


def weather_body(hours: int) -> bytes:
    return json.dumps({"hourly": {"time": [f"t{i}" for i in range(hours)]}}).encode()


def test_weather_complete_and_partial_months(ctx, fake_server):
    # ctx.today is 2026-11-06 and the archive lag is 7 days: September is complete; October
    # (ended 6 days ago) and November are partial and get dated file names.
    fake_server.add("/archive", Reply(200, weather_body(24)))
    url = fake_server.base_url + "/archive"
    stats = open_meteo.run(ctx, date(2026, 9, 1), date(2026, 11, 30), url=url)
    assert stats.fetched == 3 and stats.ok
    keys = sorted(m["key"] for m in ctx.store.iter_meta())
    assert keys == [
        "source=open_meteo_hourly/month=2026-09/open_meteo_boston_2026-09.json",
        "source=open_meteo_hourly/month=2026-10/open_meteo_boston_2026-10_partial_20261106.json",
        "source=open_meteo_hourly/month=2026-11/open_meteo_boston_2026-11_partial_20261106.json",
    ]
    nov = [h for h in fake_server.hits("/archive") if h["query"]["start_date"] == ["2026-11-01"]][0]
    assert nov["query"]["end_date"] == ["2026-11-04"]  # clipped to today - 2 days
    assert nov["query"]["timezone"] == ["GMT"]

    # same day re-run: everything skipped; complete month never re-requested
    again = open_meteo.run(ctx, date(2026, 9, 1), date(2026, 11, 30), url=url)
    assert again.fetched == 0 and again.skipped == 3
    assert len(fake_server.hits("/archive")) == 3


def test_weather_partial_month_refetched_later(ctx, fake_server):
    from datetime import timedelta

    fake_server.add("/archive", Reply(200, weather_body(24)))
    url = fake_server.base_url + "/archive"
    open_meteo.run(ctx, date(2026, 10, 1), date(2026, 10, 31), url=url)
    ctx.now = ctx.now + timedelta(days=3)  # October is now complete
    later = open_meteo.run(ctx, date(2026, 10, 1), date(2026, 10, 31), url=url)
    assert later.fetched == 1
    keys = sorted(m["key"] for m in ctx.store.iter_meta())
    assert keys[0].endswith("open_meteo_boston_2026-10.json")  # final file
    assert keys[1].endswith("_partial_20261106.json")  # earlier partial kept (immutable)


def test_weather_bad_payload_is_a_failure(ctx, fake_server):
    fake_server.add("/archive", Reply(200, b'{"error": true}'))
    stats = open_meteo.run(
        ctx, date(2026, 9, 1), date(2026, 9, 30), url=fake_server.base_url + "/archive"
    )
    assert stats.fetched == 0 and stats.failures


# ---------- GTFS static ----------
def gtfs_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("feed_info.txt", "feed_publisher_name,feed_version\nMBTA,Fall 2026\n")
        zf.writestr("stops.txt", 'stop_id,stop_name\n1,"Park, Street"\n2,Downtown\n')
    return buf.getvalue()


def test_gtfs_download_counts_and_etag_skip(ctx, fake_server):
    fake_server.add("/gtfs.zip", Reply(200, gtfs_zip(), {"ETag": '"abc123-3"'}))
    url = fake_server.base_url + "/gtfs.zip"
    first = gtfs_static.run(ctx, url=url)
    assert first.fetched == 1 and first.rows == 3
    meta = next(ctx.store.iter_meta())
    assert meta["feed_version"] == "Fall 2026"
    assert meta["rows_by_file"] == {"feed_info.txt": 1, "stops.txt": 2}
    assert meta["key"].endswith("MBTA_GTFS_abc1233.zip")

    second = gtfs_static.run(ctx, url=url)
    assert second.skipped == 1
    assert len(fake_server.hits("/gtfs.zip", "GET")) == 1


# ---------- MBTA V3 ----------
def test_v3_snapshot_sends_key_and_filter(ctx, fake_server):
    body = json.dumps({"data": [{"id": "1"}, {"id": "2"}]}).encode()
    fake_server.add("/facilities", Reply(200, body, {"x-ratelimit-limit": "1000"}))
    stats = mbta_v3.run(ctx, "facilities", api_key="secret", base_url=fake_server.base_url)
    assert stats.fetched == 1 and stats.rows == 2
    hit = fake_server.hits("/facilities")[0]
    assert hit["headers"]["x-api-key"] == "secret"
    assert hit["query"]["filter[type]"] == ["ELEVATOR,ESCALATOR"]
    meta = next(ctx.store.iter_meta())
    assert (
        meta["key"]
        == "source=mbta_v3_facilities/snapshot_date=2026-11-06/facilities_20261106T120000Z.json"
    )
    assert "secret" not in json.dumps(meta)


def test_v3_without_key_sends_no_header(ctx, fake_server):
    fake_server.add("/alerts", Reply(200, b'{"data": []}'))
    mbta_v3.run(ctx, "alerts", api_key=None, base_url=fake_server.base_url)
    assert "x-api-key" not in fake_server.hits("/alerts")[0]["headers"]


def test_v3_error_body_is_failure(ctx, fake_server):
    fake_server.add("/alerts", Reply(200, b'{"errors": [{"status": "400"}]}'))
    stats = mbta_v3.run(ctx, "alerts", base_url=fake_server.base_url)
    assert stats.failures and stats.fetched == 0
