"""Small synthetic bronze dataset for CI and the public dbt docs (no real data in Git).

    transitiq-silver --data-dir data_sample sample

Writes LAMP-, GTFS-, alerts-, weather- and facilities-shaped bronze files that reference each
other consistently (same route and stop ids), then the normal silver builders and `publish`
turn them into Iceberg tables and DuckDB views exactly as for real data. A few rows are
deliberately bad so quarantine paths run too. Values are synthetic: never report them as
measurements.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from transitiq_pipelines.silver.contracts import BRONZE_LAMP_ALERTS, INTEGER
from transitiq_pipelines.storage import BronzeExists, LocalBronzeStore

DAYS = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)]
ROUTES = {
    "Red": ["place-alfcl", "place-davis", "place-portr", "place-harsq", "place-cntsq"],
    "Orange": ["place-ogmnl", "place-mlmnl", "place-welln", "place-astao", "place-sull"],
}
FETCHED = "2026-11-06T00:00:00+00:00"
BUS_ROUTE = "SB1"
BUS_STOPS = ["sb-1", "sb-2", "sb-3", "sb-4"]
BUS_DAY = date(2026, 11, 3)  # a Tuesday covered by the sample collector window
# six scheduled bus trips, every 10 minutes from 08:00 local (13:00 UTC); what happened to each:
BUS_FATE = ["ran", "ran", "ran", "ghost", "canceled", "partial"]
# a rail-replacement shuttle: scheduled in GTFS, but its buses never report positions (untracked)
SHUTTLE_ROUTE = "Shuttle-Sample"
# trip 1 runs 8 minutes late (an 18-minute gap after trip 0), so trip 2 arrives right behind it:
# the classic "long wait, then two buses at once" bunch


def _platform(station: str) -> str:
    return f"{station}-01"


def _put(store: LocalBronzeStore, key: str, data: bytes, meta: dict) -> None:
    try:
        store.put_bytes(key, data, {"fetched_at": FETCHED, **meta})
    except BronzeExists:
        pass


def _parquet(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def gtfs_zip() -> bytes:
    stops = ["stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,wheelchair_boarding"]
    for i, station in enumerate(s for stations in ROUTES.values() for s in stations):
        lat, lon = 42.35 + i * 0.01, -71.06 - i * 0.005
        stops.append(f"{station},Station {i},{lat},{lon},1,,1")
        stops.append(f"{_platform(station)},Station {i} platform,{lat},{lon},0,{station},1")
    trips, stop_times = (
        ["route_id,service_id,trip_id,trip_headsign,direction_id"],
        ["trip_id,arrival_time,departure_time,stop_id,stop_sequence"],
    )
    for route, stations in ROUTES.items():
        for n in range(4):
            trip = f"{route}-sched-{n}"
            trips.append(f"{route},weekday,{trip},{stations[-1]},0")
            for seq, station in enumerate(stations, start=1):
                t = 6 * 3600 + n * 600 + seq * 120
                hhmmss = f"{t // 3600:02d}:{t % 3600 // 60:02d}:{t % 60:02d}"
                stop_times.append(f"{trip},{hhmmss},{hhmmss},{_platform(station)},{seq}")
    for n in range(len(BUS_FATE)):
        trip = f"{BUS_ROUTE}-{n}"
        trips.append(f"{BUS_ROUTE},weekday,{trip},Sample,0")
        for seq, stop in enumerate(BUS_STOPS, start=1):
            t = 8 * 3600 + n * 600 + (seq - 1) * 300
            hhmmss = f"{t // 3600:02d}:{t % 3600 // 60:02d}:{t % 60:02d}"
            stop_times.append(f"{trip},{hhmmss},{hhmmss},{stop},{seq}")
    for n in range(3):
        trip = f"{SHUTTLE_ROUTE}-{n}"
        trips.append(f"{SHUTTLE_ROUTE},weekday,{trip},Shuttle,0")
        for seq, stop in enumerate(BUS_STOPS, start=1):
            t = 8 * 3600 + 300 + n * 600 + (seq - 1) * 300
            hhmmss = f"{t // 3600:02d}:{t % 3600 // 60:02d}:{t % 60:02d}"
            stop_times.append(f"{trip},{hhmmss},{hhmmss},{stop},{seq}")
    files = {
        "feed_info.txt": "feed_publisher_name,feed_version\nMBTA,Sample feed\n",
        "routes.txt": (
            "route_id,agency_id,route_short_name,route_long_name,route_type,route_color\n"
            "Red,1,,Red Line,1,DA291C\nOrange,1,,Orange Line,1,ED8B00\n"
            f"{BUS_ROUTE},1,SB1,Sample Bus,3,FFC72C\n"
            f"{SHUTTLE_ROUTE},1,,Sample Shuttle,3,FFC72C\n"
        ),
        "stops.txt": "\n".join(stops) + "\n",
        "trips.txt": "\n".join(trips) + "\n",
        "stop_times.txt": "\n".join(stop_times) + "\n",
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "weekday,1,1,1,1,1,0,0,20261101,20261231\n"
        ),
        "calendar_dates.txt": "service_id,date,exception_type\nweekday,20261126,2\n",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return buf.getvalue()


def lamp_day(day: date) -> pa.Table:
    rows = []
    base = int(datetime(day.year, day.month, day.day, 11, 0, tzinfo=UTC).timestamp())  # 07:00 local
    for r_i, (route, stations) in enumerate(ROUTES.items()):
        for n in range(8):
            # trains every 6 minutes; train 5 runs right behind train 4 (a bunch)
            start = base + n * 360 - (300 if n == 5 else 0) + r_i * 30
            for seq, station in enumerate(stations, start=1):
                arrive = start + seq * 130
                rows.append(
                    {
                        "service_date": int(day.strftime("%Y%m%d")),
                        "route_id": route,
                        "trunk_route_id": route,
                        "branch_route_id": None,
                        "direction_id": True,
                        "direction": "North",
                        "direction_destination": stations[-1],
                        "trip_id": f"{route}-{day:%m%d}-{n}",
                        "vehicle_id": f"{route[0]}-{n}",
                        "vehicle_label": str(1000 + n),
                        "stop_id": _platform(station),
                        "parent_station": station,
                        "stop_sequence": seq * 10,
                        "stop_count": len(stations),
                        "move_timestamp": arrive - 130 if seq > 1 else None,
                        "stop_timestamp": arrive,
                        "travel_time_seconds": 130 if seq > 1 else None,
                        "dwell_time_seconds": 30,
                        "headway_trunk_seconds": (60 if n == 5 else 360) if n > 0 else None,
                        "headway_branch_seconds": None,
                        "start_time": 25200 + n * 360,
                        "scheduled_arrival_time": 25200 + n * 360 + seq * 120,
                        "scheduled_departure_time": 25200 + n * 360 + seq * 120 + 30,
                        "scheduled_travel_time": 120 if seq > 1 else None,
                        "scheduled_headway_trunk": 360,
                        "scheduled_headway_branch": None,
                    }
                )
    rows.append({**rows[3]})  # exact duplicate -> quarantined as duplicate_event
    rows.append({**rows[4], "trip_id": "bad-dwell", "dwell_time_seconds": 99999})
    types = {
        "service_date": pa.int64(),
        "direction_id": pa.bool_(),
        "stop_sequence": pa.int16(),
        "stop_count": pa.int16(),
    }
    ints = {
        "move_timestamp",
        "stop_timestamp",
        "travel_time_seconds",
        "dwell_time_seconds",
        "headway_trunk_seconds",
        "headway_branch_seconds",
        "start_time",
        "scheduled_arrival_time",
        "scheduled_departure_time",
        "scheduled_travel_time",
        "scheduled_headway_trunk",
        "scheduled_headway_branch",
    }
    cols = {}
    for name in rows[0]:
        typ = types.get(name, pa.int64() if name in ints else pa.string())
        cols[name] = pa.array([r[name] for r in rows], typ)
    return pa.table(cols)


def alerts_table() -> pa.Table:
    t0 = int(datetime(2026, 11, 2, 12, 0, tzinfo=UTC).timestamp())
    station = ROUTES["Red"][1]
    base = {name: None for name in BRONZE_LAMP_ALERTS}
    rows = [
        {
            **base,
            "id": 1,
            "cause": "UNKNOWN_CAUSE",
            "effect": "OTHER_EFFECT",
            "effect_detail": "ELEVATOR_CLOSURE",
            "severity": 1,
            "severity_level": "INFO",
            "header_text.translation.text": "Sample elevator 802 unavailable",
            "created_timestamp": t0,
            "last_modified_timestamp": t0,
            "active_period.start_timestamp": t0,
            "closed_timestamp": t0 + 7200,
            "informed_entity.stop_id": _platform(station),
            "informed_entity.facility_id": "802",
            "informed_entity.activities": "USING_WHEELCHAIR",
        },
        {
            **base,
            "id": 2,
            "cause": "TECHNICAL_PROBLEM",
            "effect": "SIGNIFICANT_DELAYS",
            "effect_detail": "DELAY",
            "severity": 7,
            "severity_level": "WARNING",
            "header_text.translation.text": "Sample Red Line delays",
            "created_timestamp": t0,
            "last_modified_timestamp": t0,
            "active_period.start_timestamp": t0,
            "active_period.end_timestamp": t0 + 3600,
            "informed_entity.route_id": "Red",
            "informed_entity.route_type": 1,
        },
        {
            **base,
            "id": 3,
            "effect": "DETOUR",
            "created_timestamp": t0,
            "last_modified_timestamp": t0,
            "active_period.start_timestamp": t0,
            "active_period.end_timestamp": t0 - 60,
        },  # rejected
    ]
    types = {
        n: (pa.int64() if f == INTEGER else pa.string()) for n, f in BRONZE_LAMP_ALERTS.items()
    }
    return pa.table({n: pa.array([r[n] for r in rows], types[n]) for n in types})


def weather_month() -> dict:
    start = datetime(2026, 11, 1)
    hours = [start + timedelta(hours=h) for h in range(24 * 6)]
    return {
        "hourly": {
            "time": [h.strftime("%Y-%m-%dT%H:%M") for h in hours],
            "temperature_2m": [5.0 + (h.hour % 12) for h in hours],
            "precipitation": [0.5 if h.day == 3 else 0.0 for h in hours],
            "rain": [0.5 if h.day == 3 else 0.0 for h in hours],
            "snowfall": [0.0] * len(hours),
            "weather_code": [61 if h.day == 3 else 1 for h in hours],
        }
    }


def facilities_json() -> dict:
    station = ROUTES["Red"][1]
    return {
        "data": [
            {
                "id": "802",
                "attributes": {"type": "ELEVATOR", "long_name": "Sample Elevator 802"},
                "relationships": {"stop": {"data": {"id": station}}},
            }
        ]
    }


def realtime_tables() -> tuple[pa.Table, pa.Table]:
    """Collector-shaped VP/TU rows: snapshots every minute 12:50-14:20 UTC on BUS_DAY."""
    from transitiq_pipelines.realtime.feeds import TU_SCHEMA, VP_SCHEMA

    vp, tu = [], []
    start = datetime(BUS_DAY.year, BUS_DAY.month, BUS_DAY.day, 12, 50, tzinfo=UTC)
    sched0 = datetime(BUS_DAY.year, BUS_DAY.month, BUS_DAY.day, 13, 0, tzinfo=UTC)
    for minute in range(91):
        ts = start + timedelta(minutes=minute)
        for n, fate in enumerate(BUS_FATE):
            trip = f"{BUS_ROUTE}-{n}"
            late = timedelta(minutes=8) if n == 1 else timedelta(0)
            t_start = sched0 + timedelta(minutes=10 * n) + late
            progress = (ts - t_start) / timedelta(minutes=5)  # stops passed
            base = {
                "trip_id": trip,
                "route_id": BUS_ROUTE,
                "direction_id": 0,
                "start_date": BUS_DAY.strftime("%Y%m%d"),
                "start_time": (sched0 + timedelta(minutes=10 * n)).strftime("%H:%M:%S"),
            }
            if fate == "canceled":
                if ts < t_start:
                    tu.append(
                        {
                            **base,
                            "feed_ts": ts,
                            "schedule_relationship": "CANCELED",
                            "stop_updates": 0,
                        }
                    )
                continue
            if progress > len(BUS_STOPS) - 1 or progress < -10:
                continue
            next_seq = max(1, int(progress) + 2) if progress >= 0 else 1
            if fate == "partial" and next_seq > 2:
                continue  # vehicle vanishes after stop 1
            if next_seq > len(BUS_STOPS):
                continue
            eta = t_start + timedelta(minutes=5 * (next_seq - 1))
            tu.append(
                {
                    **base,
                    "feed_ts": ts,
                    "schedule_relationship": "SCHEDULED",
                    "vehicle_id": None if fate == "ghost" else f"bus-{n}",
                    "stop_updates": len(BUS_STOPS) - next_seq + 1,
                    "next_stop_sequence": next_seq,
                    "next_stop_id": BUS_STOPS[next_seq - 1],
                    "next_arrival_ts": eta,
                    "next_departure_ts": eta,
                }
            )
            if fate != "ghost" and progress >= 0:
                vp.append(
                    {
                        **base,
                        "feed_ts": ts,
                        "vehicle_id": f"bus-{n}",
                        "vehicle_label": str(n),
                        "schedule_relationship": "SCHEDULED",
                        "latitude": 42.3,
                        "longitude": -71.1,
                        "current_stop_sequence": next_seq,
                        "stop_id": BUS_STOPS[next_seq - 1],
                        "current_status": "IN_TRANSIT_TO",
                        "vehicle_ts": ts,
                    }
                )
        # an unrelated subway vehicle keeps the collector "alive" every minute
        vp.append(
            {
                "feed_ts": ts,
                "vehicle_id": "R-1",
                "trip_id": "ADDED-1",
                "route_id": "Red",
                "schedule_relationship": "ADDED",
                "latitude": 42.35,
                "longitude": -71.06,
                "current_status": "IN_TRANSIT_TO",
                "vehicle_ts": ts,
            }
        )
    return (pa.Table.from_pylist(vp, schema=VP_SCHEMA), pa.Table.from_pylist(tu, schema=TU_SCHEMA))


def write_sample_bronze(store: LocalBronzeStore) -> None:
    _put(
        store,
        "source=gtfs_static/fetched_date=2026-11-06/MBTA_GTFS_sample.zip",
        gtfs_zip(),
        {"source": "gtfs_static", "feed_version": "Sample feed"},
    )
    for day in DAYS:
        _put(
            store,
            f"source=lamp_subway_otp/service_date={day}/{day}-subway-on-time-performance-v1"
            "_v20261106T000000Z.parquet",
            _parquet(lamp_day(day)),
            {"source": "lamp_subway_otp", "upstream_last_modified": "2026-11-06T00:00:00+00:00"},
        )
    _put(
        store,
        "source=lamp_alerts/snapshot_date=2026-11-06/LAMP_RT_ALERTS_vsample.parquet",
        _parquet(alerts_table()),
        {"source": "lamp_alerts"},
    )
    _put(
        store,
        "source=open_meteo_hourly/month=2026-11/open_meteo_boston_2026-11_partial_20261106.json",
        json.dumps(weather_month()).encode(),
        {"source": "open_meteo_hourly", "month": "2026-11", "complete": False},
    )
    _put(
        store,
        "source=mbta_v3_facilities/snapshot_date=2026-11-06/facilities_sample.json",
        json.dumps(facilities_json()).encode(),
        {"source": "mbta_v3_facilities"},
    )
    vp, tu = realtime_tables()
    _put(
        store,
        f"source=gtfs_rt_vehicle_positions/ingest_date={BUS_DAY}/vp_sample.parquet",
        _parquet(vp),
        {"source": "gtfs_rt_vehicle_positions", "rows": vp.num_rows},
    )
    _put(
        store,
        f"source=gtfs_rt_trip_updates/ingest_date={BUS_DAY}/tu_sample.parquet",
        _parquet(tu),
        {"source": "gtfs_rt_trip_updates", "rows": tu.num_rows},
    )
