from datetime import date

import duckdb
import pytest
from transitiq_pipelines.gold import quality as Q


@pytest.fixture
def conn():
    c = duckdb.connect()
    c.execute("create schema silver; create schema gold")
    # 10 days x 100 events, newest day 2026-11-08 with only 20 events
    c.execute("""
        create table silver.subway_events as
        select (date '2026-10-30' + to_days(d))::date as service_date, i as n
        from range(10) t(d), range(100) u(i)
        where not (d = 9 and i >= 20)
    """)
    c.execute("""
        create table gold.fact_travel_time as
        select service_date, 't' || n as trip_id, 'v' as vehicle_id, 's' as to_stop_id,
               timestamptz '2026-11-01 00:00:00+00' + to_seconds(n) as departure_ts,
               case when n % 10 = 0 then null else 120 end as scheduled_seconds
        from silver.subway_events where n < 90
    """)
    c.execute("""
        create table gold.dim_weather_hour as
        select (date '2026-10-30' + to_days(h // 24))::date as local_date from range(10 * 24) t(h)
    """)
    return c


def test_freshness(conn):
    assert Q.check_freshness(conn, date(2026, 11, 9)).status == Q.PASS
    assert Q.check_freshness(conn, date(2026, 11, 12)).status == Q.WARN
    assert Q.check_freshness(conn, date(2026, 11, 30)).status == Q.FAIL


def test_volume_flags_a_short_day(conn):
    r = Q.check_volume(conn)
    assert r.status == Q.FAIL and r.value == 0.2


def test_silver_gold_ratio(conn):
    r = Q.check_silver_gold(conn)
    assert r.status == Q.PASS and 0.8 <= r.value <= 1.0


def test_duplicates_and_nulls(conn):
    assert Q.check_duplicates(conn).status == Q.PASS
    conn.execute("insert into gold.fact_travel_time select * from gold.fact_travel_time limit 1")
    assert Q.check_duplicates(conn).status == Q.FAIL
    assert Q.check_nulls(conn).status == Q.PASS  # ~10% < 15%


def test_weather_coverage(conn):
    assert Q.check_weather_coverage(conn).status == Q.PASS
    conn.execute("delete from gold.dim_weather_hour where local_date > date '2026-11-04'")
    assert Q.check_weather_coverage(conn).status == Q.WARN


def test_bronze_silver_reconciliation(tmp_path):
    from transitiq_pipelines.checkpoint import Checkpoint
    from transitiq_pipelines.storage import LocalBronzeStore

    store = LocalBronzeStore(tmp_path / "bronze")
    store.put_bytes("source=lamp_subway_otp/service_date=2026-11-01/a.parquet", b"x", {"rows": 10})
    store.put_bytes("source=lamp_subway_otp/service_date=2026-11-02/b.parquet", b"x", {"rows": 7})
    ck = Checkpoint(tmp_path / "ck", "silver_subway_events")
    ck.mark(
        "2026-11-01",
        signature="source=lamp_subway_otp/service_date=2026-11-01/a.parquet",
        rows_in=10,
    )
    ck.mark(
        "2026-11-02",
        signature="source=lamp_subway_otp/service_date=2026-11-02/b.parquet",
        rows_in=6,
    )
    r = Q.check_bronze_silver(store, tmp_path / "ck")
    assert r.status == Q.FAIL and "2026-11-02" in r.detail


def test_volume_compares_same_weekday():
    """Regression: a normal Sunday must not be flagged against weekday volumes."""
    c = duckdb.connect()
    c.execute("create schema silver")
    # 28 days ending Sunday 2026-11-08: weekdays 100 rows, Sundays 50 rows
    c.execute("""
        create table silver.subway_events as
        select d::date as service_date, n
        from generate_series(date '2026-10-12', date '2026-11-08', interval 1 day) t(d),
             range(100) u(n)
        where isodow(d) <> 7 or n < 50
    """)
    r = Q.check_volume(c)
    assert r.status == Q.PASS and r.value == 1.0 and "Sundays" in r.detail
