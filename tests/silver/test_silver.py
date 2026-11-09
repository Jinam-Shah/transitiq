"""Silver layer tests: contracts, validation rules, idempotent replays, quarantine.

Iceberg tests are skipped on Windows (PyIceberg path bug, D-012); run the full suite with
`docker compose run --rm pipelines pytest` or rely on CI (Linux).
"""

import io
import json
import sys
import zipfile
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from transitiq_pipelines.silver import contracts as C
from transitiq_pipelines.silver import engine
from transitiq_pipelines.silver.builders import (
    SUBWAY_SQL,
    _sql_literal,
    weather_inputs,
)
from transitiq_pipelines.storage import LocalBronzeStore

windows_skip = pytest.mark.skipif(
    sys.platform == "win32",
    reason="PyIceberg cannot handle Windows paths (apache/iceberg-python#3720)",
)
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
DAY_TS = int(datetime(2026, 11, 4, 12, 0, tzinfo=UTC).timestamp())


# ---------- fixtures ----------
def lamp_row(**over):
    row = {
        "service_date": 20261104,
        "route_id": "Red",
        "trunk_route_id": "Red",
        "branch_route_id": None,
        "direction_id": True,
        "direction": "North",
        "direction_destination": "Alewife",
        "trip_id": "t1",
        "vehicle_id": "v1",
        "vehicle_label": "1800",
        "stop_id": "70061",
        "parent_station": "place-alfcl",
        "stop_sequence": 1,
        "stop_count": 20,
        "move_timestamp": DAY_TS,
        "stop_timestamp": DAY_TS + 90,
        "travel_time_seconds": 90,
        "dwell_time_seconds": 30,
        "headway_trunk_seconds": 300,
        "headway_branch_seconds": None,
        "start_time": 43000,
        "scheduled_arrival_time": 43200,
        "scheduled_departure_time": 43230,
        "scheduled_travel_time": 100,
        "scheduled_headway_trunk": 360,
        "scheduled_headway_branch": None,
        "occupancy_status": "MANY_SEATS_AVAILABLE",  # extra upstream column: allowed
    }
    row.update(over)
    return row


LAMP_TYPES = {
    "service_date": pa.int64(),
    "direction_id": pa.bool_(),
    "stop_sequence": pa.int16(),
    "stop_count": pa.int16(),
}


def lamp_table(rows):
    """Arrow table shaped like a real LAMP file (types follow the bronze contract)."""
    cols = {}
    for name in rows[0]:
        typ = LAMP_TYPES.get(name)
        if typ is None:
            sample = next((r[name] for r in rows if r[name] is not None), None)
            is_int = C.BRONZE_LAMP_SUBWAY.get(name) == C.INTEGER
            typ = pa.string() if isinstance(sample, str) or not is_int else pa.int64()
        cols[name] = pa.array([r[name] for r in rows], typ)
    return pa.table(cols)


def put_lamp(store, day, rows, version="20261105T000000Z"):
    key = (
        f"source=lamp_subway_otp/service_date={day}/"
        f"{day}-subway-on-time-performance-v1_v{version}.parquet"
    )
    buf = io.BytesIO()
    pq.write_table(lamp_table(rows), buf)
    store.put_bytes(
        key,
        buf.getvalue(),
        {"source": "lamp_subway_otp", "upstream_last_modified": version, "sha256": "x" * 64},
    )
    return key


# ---------- contracts ----------
def test_bronze_contract_accepts_real_shape_and_widening():
    C.check_bronze(lamp_table([lamp_row()]).schema, C.BRONZE_LAMP_SUBWAY, "lamp")


def test_bronze_contract_detects_missing_column():
    table = lamp_table([lamp_row()]).drop_columns(["travel_time_seconds"])
    with pytest.raises(C.ContractError, match="missing column 'travel_time_seconds'"):
        C.check_bronze(table.schema, C.BRONZE_LAMP_SUBWAY, "lamp")


def test_bronze_contract_detects_type_change():
    table = lamp_table([lamp_row(travel_time_seconds="90")])
    with pytest.raises(C.ContractError, match="travel_time_seconds"):
        C.check_bronze(table.schema, C.BRONZE_LAMP_SUBWAY, "lamp")


def test_same_schema_ignores_large_string():
    a = pa.schema([("x", pa.large_string())])
    assert C.same_schema(a, pa.schema([("x", pa.string())])) == []
    assert C.same_schema(a, pa.schema([("x", pa.int32())])) == ["x: string != int32"]


# ---------- subway rules (DuckDB only, runs everywhere) ----------
def run_subway(tmp_path, rows):
    # Path deliberately contains "service_date=..." to guard against DuckDB hive-partition
    # detection replacing the real column (a bug found on real data).
    folder = tmp_path / "service_date=2026-11-04"
    folder.mkdir()
    path = folder / "f.parquet"
    pq.write_table(lamp_table(rows), path)
    sql = SUBWAY_SQL.format(path=_sql_literal(path), expected="'2026-11-04'")
    return engine.split(engine.connect(), sql, C.SUBWAY_EVENTS, "key", "sha", NOW)


def test_subway_rules(tmp_path):
    rows = [
        lamp_row(trip_id="ok"),
        lamp_row(trip_id="ok"),  # exact repeat -> duplicate_event
        lamp_row(trip_id="neg", travel_time_seconds=-5),
        lamp_row(trip_id="nots", move_timestamp=None, stop_timestamp=None),
        lamp_row(trip_id="dwell", dwell_time_seconds=9000),
        lamp_row(trip_id="far", stop_timestamp=DAY_TS + 5 * 86400),
        lamp_row(trip_id="wrongday", service_date=20261103),
        lamp_row(trip_id="", stop_id="x"),
        lamp_row(trip_id="first_stop", travel_time_seconds=None),  # legit: first stop of trip
    ]
    result = run_subway(tmp_path, rows)
    assert sorted(result.valid.column("trip_id").to_pylist()) == ["first_stop", "ok"]
    assert dict(result.reasons) == {
        "duplicate_event": 1,
        "negative_duration": 1,
        "no_timestamps": 1,
        "implausible_dwell_time": 1,
        "timestamp_outside_service_day": 1,
        "service_date_mismatch": 1,
        "missing_field:trip_id": 1,
    }
    assert result.valid.schema == C.SUBWAY_EVENTS
    assert set(result.rejected.column_names) >= {"_reason", "_row_number", "_source_file"}


def test_subway_types_and_lineage(tmp_path):
    result = run_subway(tmp_path, [lamp_row()])
    row = result.valid.to_pylist()[0]
    assert str(row["service_date"]) == "2026-11-04"
    assert row["direction_id"] == 1
    assert row["stop_ts"] == datetime.fromtimestamp(DAY_TS + 90, UTC)
    assert row["_source_file"] == "key" and row["_loaded_at"] == NOW


# ---------- weather input selection ----------
def test_weather_prefers_complete_month():
    metas = [
        {"key": "a", "month": "2026-10", "complete": False, "fetched_at": "2026-11-05"},
        {"key": "b", "month": "2026-10", "complete": True, "fetched_at": "2026-11-08"},
        {"key": "c", "month": "2026-11", "complete": False, "fetched_at": "2026-11-05"},
        {"key": "d", "month": "2026-11", "complete": False, "fetched_at": "2026-11-08"},
    ]
    assert {m: v["key"] for m, v in weather_inputs(metas).items()} == {
        "2026-10": "b",
        "2026-11": "d",
    }


# ---------- Iceberg end to end (Linux only) ----------
@pytest.fixture
def silver(tmp_path):
    from transitiq_pipelines.silver.builders import SilverContext
    from transitiq_pipelines.silver.warehouse import Warehouse

    store = LocalBronzeStore(tmp_path / "bronze")

    def make(force=False):
        return SilverContext(
            store=store,
            warehouse=Warehouse(tmp_path),
            checkpoint_dir=tmp_path / "checkpoints",
            force=force,
            now=NOW,
        )

    return store, make


@windows_skip
def test_subway_replay_is_idempotent(silver):
    from transitiq_pipelines.silver.builders import build_subway_events

    store, make = silver
    put_lamp(
        store,
        "2026-11-04",
        [lamp_row(trip_id=f"t{i}") for i in range(5)] + [lamp_row(trip_id="t0")],
    )
    put_lamp(
        store,
        "2026-11-05",
        [lamp_row(trip_id="x", service_date=20261105, stop_timestamp=DAY_TS + 86400)],
    )

    first = build_subway_events(make())
    assert (first.partitions_loaded, first.rows_valid, first.rows_quarantined) == (2, 6, 1)
    ctx = make()
    assert ctx.warehouse.record_count("subway_events") == 6

    again = build_subway_events(make())
    assert (again.partitions_loaded, again.partitions_skipped) == (0, 2)
    forced = build_subway_events(make(force=True))
    assert forced.partitions_loaded == 2
    assert make().warehouse.record_count("subway_events") == 6  # no duplicates after replay

    schema = make().warehouse.catalog.load_table("silver.subway_events").schema().as_arrow()
    assert C.same_schema(schema, C.SUBWAY_EVENTS) == []
    q = list((store.root.parent / "quarantine" / "subway_events").glob("*.parquet"))
    assert [p.name for p in q] == ["service_date=2026-11-04.parquet"]


@windows_skip
def test_republished_day_replaces_only_that_partition(silver):
    from pyiceberg.expressions import EqualTo
    from transitiq_pipelines.silver.builders import build_subway_events

    store, make = silver
    put_lamp(store, "2026-11-04", [lamp_row(trip_id=f"a{i}") for i in range(3)])
    put_lamp(
        store,
        "2026-11-05",
        [lamp_row(trip_id="b", service_date=20261105, stop_timestamp=DAY_TS + 86400)],
    )
    build_subway_events(make())
    put_lamp(store, "2026-11-04", [lamp_row(trip_id="new")], version="20261109T000000Z")
    stats = build_subway_events(make())
    assert (stats.partitions_loaded, stats.partitions_skipped) == (1, 1)
    wh = make().warehouse
    day = wh.read("subway_events", EqualTo("service_date", "2026-11-04"))
    assert day.column("trip_id").to_pylist() == ["new"]
    assert wh.record_count("subway_events") == 2


@windows_skip
def test_schema_drift_fails_partition_and_keeps_old_data(silver):
    from transitiq_pipelines.silver.builders import build_subway_events

    store, make = silver
    put_lamp(store, "2026-11-04", [lamp_row()])
    build_subway_events(make())
    put_lamp(store, "2026-11-04", [lamp_row(travel_time_seconds="90")], version="20261110T000000Z")
    stats = build_subway_events(make())
    assert "schema drift" in stats.failures[0]["error"]
    assert make().warehouse.record_count("subway_events") == 1


def gtfs_zip():
    files = {
        "feed_info.txt": "feed_publisher_name,feed_version\nMBTA,Fall 2026\n",
        "routes.txt": (
            "route_id,agency_id,route_long_name,route_type\nRed,1,Red Line,1\nBad,1,X,subway\n"
        ),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n1,Park,42.35,-71.06\n,NoId,42,-71\n",
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id\n"
            'Red,wk,t1,"Burlington via Medford Square, West Cummings & Woburn (Express)",0\n'
            "Red,wk,t1,dup,1\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "t1,25:10:00,25:11:00,1,1\nt1,9am,09:00:00,1,2\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "wk,1,1,1,1,1,0,0,20261101,20261231\n"
        ),
        "calendar_dates.txt": "service_id,date,exception_type\nwk,20261126,2\n",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return buf.getvalue()


@windows_skip
def test_gtfs_rules(silver):
    from transitiq_pipelines.silver.builders import build_gtfs

    store, make = silver
    store.put_bytes(
        "source=gtfs_static/fetched_date=2026-11-09/MBTA_GTFS_abc.zip",
        gtfs_zip(),
        {"source": "gtfs_static", "feed_version": "Fall 2026", "fetched_at": "2026-11-09"},
    )
    stats = {s.table: s for s in build_gtfs(make())}
    assert all(s.ok for s in stats.values()), [s.failures for s in stats.values()]
    assert dict(stats["gtfs_routes"].reasons) == {"invalid_type:route_type": 1}
    assert dict(stats["gtfs_stops"].reasons) == {"missing_field:stop_id": 1}
    assert dict(stats["gtfs_trips"].reasons) == {"duplicate_key": 1}  # quoted comma parsed fine
    assert dict(stats["gtfs_stop_times"].reasons) == {"invalid_time:arrival_time": 1}
    times = make().warehouse.read("gtfs_stop_times").to_pylist()
    assert times[0]["arrival_seconds"] == 25 * 3600 + 600  # after-midnight times kept
    assert times[0]["feed_version"] == "Fall 2026"
    cal = make().warehouse.read("gtfs_calendar").to_pylist()[0]
    assert str(cal["start_date"]) == "2026-11-01"


@windows_skip
def test_weather_and_facilities(silver):
    from transitiq_pipelines.silver.builders import build_facilities, build_weather_hourly

    store, make = silver
    hours = ["2026-10-01T00:00", "2026-10-01T01:00", "2026-10-01T01:00", "2026-10-01T02:00"]
    payload = {
        "hourly": {
            "time": hours,
            "temperature_2m": [10.0, 11.0, 11.0, None],
            "precipitation": [0.0, -1.0, 0.0, 0.0],
            "weather_code": [0, 1, 1, 2],
        }
    }
    store.put_bytes(
        "source=open_meteo_hourly/month=2026-10/open_meteo_boston_2026-10.json",
        json.dumps(payload).encode(),
        {"month": "2026-10", "complete": True, "fetched_at": "2026-11-09"},
    )
    wx = build_weather_hourly(make())
    assert wx.rows_valid == 1
    assert dict(wx.reasons) == {
        "negative_precipitation": 1,
        "missing_field:temperature_2m": 1,
        "duplicate_hour": 1,
    }

    fac = {
        "data": [
            {
                "id": "802",
                "attributes": {"type": "ELEVATOR", "long_name": "State 802"},
                "relationships": {"stop": {"data": {"id": "place-state"}}},
            },
            {"id": "803", "attributes": {"type": "ELEVATOR", "latitude": 10.0, "longitude": 10.0}},
            {"id": "", "attributes": {"type": "ESCALATOR"}},
        ]
    }
    store.put_bytes(
        "source=mbta_v3_facilities/snapshot_date=2026-11-09/facilities_x.json",
        json.dumps(fac).encode(),
        {"fetched_at": "2026-11-09"},
    )
    stats = build_facilities(make())
    assert stats.rows_valid == 1
    assert dict(stats.reasons) == {"coordinates_out_of_range": 1, "missing_field:facility_id": 1}
    row = make().warehouse.read("facilities").to_pylist()[0]
    assert (row["facility_id"], row["stop_id"]) == ("802", "place-state")


def test_cli_refuses_on_windows(monkeypatch, capsys, tmp_path):
    from transitiq_pipelines.silver import cli

    monkeypatch.setattr(cli.sys, "platform", "win32")
    assert cli.main(["--data-dir", str(tmp_path), "build"]) == 2
    assert "docker compose run --rm pipelines" in capsys.readouterr().err


def test_rules_do_not_import_pyiceberg():
    """Validation rules must load without PyIceberg/SQLAlchemy, which Windows may block
    (Smart App Control blocked SQLAlchemy's compiled extension on the dev laptop)."""
    import subprocess

    code = (
        "import sys, transitiq_pipelines.silver.builders, transitiq_pipelines.silver.cli; "
        "bad = [m for m in ('pyiceberg', 'sqlalchemy') if m in sys.modules]; "
        "sys.exit(1 if bad else 0)"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


def test_alert_rules(tmp_path):
    from transitiq_pipelines.silver.builders import ALERTS_SQL

    def alert(i, **over):
        row = {name: None for name in C.BRONZE_LAMP_ALERTS}
        row.update(
            {
                "id": i,
                "effect": "OTHER_EFFECT",
                "effect_detail": "ELEVATOR_CLOSURE",
                "severity": 1,
                "created_timestamp": DAY_TS,
                "last_modified_timestamp": DAY_TS,
                "active_period.start_timestamp": DAY_TS,
                "active_period.end_timestamp": DAY_TS + 3600,
                "informed_entity.facility_id": "802",
            }
        )
        row.update(over)
        return row

    rows = [
        alert(1),
        alert(1),  # exact duplicate version
        alert(2, **{"active_period.start_timestamp": None, "active_period.end_timestamp": None}),
        alert(3, **{"active_period.end_timestamp": DAY_TS - 60}),
        alert(4, effect=None),
    ]
    types = {
        n: (pa.int64() if f == C.INTEGER else pa.string()) for n, f in C.BRONZE_LAMP_ALERTS.items()
    }
    table = pa.table({n: pa.array([r[n] for r in rows], types[n]) for n in types})
    folder = tmp_path / "snapshot_date=2026-11-09"
    folder.mkdir()
    pq.write_table(table, folder / "a.parquet")
    sql = ALERTS_SQL.format(path=_sql_literal(folder / "a.parquet"))
    result = engine.split(engine.connect(), sql, C.ALERTS, "key", "sha", NOW)
    assert sorted(result.valid.column("alert_id").to_pylist()) == [1, 2]  # null start is kept
    assert dict(result.reasons) == {
        "duplicate_row": 1,
        "end_before_start": 1,
        "missing_field:effect": 1,
    }
