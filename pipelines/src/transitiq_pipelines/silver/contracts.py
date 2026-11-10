"""Data contracts: the exact schemas silver tables must have, and what bronze files must contain.

These schemas are the single source of truth. The Iceberg tables are created from them,
every write is cast to them, tests compare the live tables against them, and Phase 4's dbt
sources are checked against them. Changing a type here is a deliberate, reviewed change;
upstream changing a type is caught by check_bronze() and fails the partition loudly.
"""

from __future__ import annotations

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")
STR = pa.string()
I32 = pa.int32()
I64 = pa.int64()
F64 = pa.float64()

LINEAGE = [
    pa.field("_source_file", STR),  # bronze key the row came from
    pa.field("_source_sha256", STR),  # checksum of that bronze file (null for small JSON)
    pa.field("_loaded_at", TS),  # when this silver load ran
]


def _schema(*fields: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t in fields] + LINEAGE)


# LAMP subway on-time performance. Integer time fields keep LAMP's names and units
# (seconds); see https://github.com/mbta/lamp/blob/main/Data_Dictionary.md.
SUBWAY_EVENTS = _schema(
    ("service_date", pa.date32()),
    ("route_id", STR),
    ("trunk_route_id", STR),
    ("branch_route_id", STR),
    ("direction_id", I32),
    ("direction", STR),
    ("direction_destination", STR),
    ("trip_id", STR),
    ("vehicle_id", STR),
    ("vehicle_label", STR),
    ("stop_id", STR),
    ("parent_station", STR),
    ("stop_sequence", I32),
    ("stop_count", I32),
    ("move_ts", TS),
    ("stop_ts", TS),
    ("travel_time_seconds", I64),
    ("dwell_time_seconds", I64),
    ("headway_trunk_seconds", I64),
    ("headway_branch_seconds", I64),
    ("start_time", I64),
    ("scheduled_arrival_time", I64),
    ("scheduled_departure_time", I64),
    ("scheduled_travel_time", I64),
    ("scheduled_headway_trunk", I64),
    ("scheduled_headway_branch", I64),
)

ALERTS = _schema(
    ("alert_id", I64),
    ("cause", STR),
    ("cause_detail", STR),
    ("effect", STR),
    ("effect_detail", STR),
    ("severity_level", STR),
    ("severity", I32),
    ("alert_lifecycle", STR),
    ("duration_certainty", STR),
    ("header_text", STR),
    ("created_ts", TS),
    ("last_modified_ts", TS),
    ("closed_ts", TS),
    ("active_start_ts", TS),
    ("active_end_ts", TS),
    ("route_id", STR),
    ("route_type", I32),
    ("direction_id", I32),
    ("stop_id", STR),
    ("facility_id", STR),
    ("activities", STR),
)

WEATHER_HOURLY = _schema(
    ("hour_ts", TS),
    ("temperature_2m_c", F64),
    ("relative_humidity_2m_pct", F64),
    ("precipitation_mm", F64),
    ("rain_mm", F64),
    ("snowfall_cm", F64),
    ("snow_depth_m", F64),
    ("weather_code", I32),
    ("wind_speed_10m_kmh", F64),
    ("wind_gusts_10m_kmh", F64),
)

FACILITIES = _schema(
    ("facility_id", STR),
    ("facility_type", STR),
    ("long_name", STR),
    ("short_name", STR),
    ("stop_id", STR),
    ("latitude", F64),
    ("longitude", F64),
)

_FEED = [("feed_version", STR)]

GTFS_ROUTES = _schema(
    ("route_id", STR),
    ("agency_id", STR),
    ("route_short_name", STR),
    ("route_long_name", STR),
    ("route_desc", STR),
    ("route_type", I32),
    ("route_color", STR),
    ("route_text_color", STR),
    ("route_sort_order", I32),
    ("line_id", STR),
    *_FEED,
)

GTFS_STOPS = _schema(
    ("stop_id", STR),
    ("stop_name", STR),
    ("platform_code", STR),
    ("platform_name", STR),
    ("stop_lat", F64),
    ("stop_lon", F64),
    ("location_type", I32),
    ("parent_station", STR),
    ("wheelchair_boarding", I32),
    ("municipality", STR),
    ("vehicle_type", I32),
    *_FEED,
)

GTFS_TRIPS = _schema(
    ("trip_id", STR),
    ("route_id", STR),
    ("service_id", STR),
    ("trip_headsign", STR),
    ("trip_short_name", STR),
    ("direction_id", I32),
    ("block_id", STR),
    ("shape_id", STR),
    ("wheelchair_accessible", I32),
    ("route_pattern_id", STR),
    *_FEED,
)

GTFS_STOP_TIMES = _schema(
    ("trip_id", STR),
    ("stop_sequence", I32),
    ("stop_id", STR),
    ("arrival_time", STR),  # GTFS "HH:MM:SS"; may exceed 24:00:00 after midnight
    ("departure_time", STR),
    ("arrival_seconds", I32),  # seconds after midnight of the service day
    ("departure_seconds", I32),
    ("pickup_type", I32),
    ("drop_off_type", I32),
    ("timepoint", I32),
    *_FEED,
)

GTFS_CALENDAR = _schema(
    ("service_id", STR),
    ("monday", I32),
    ("tuesday", I32),
    ("wednesday", I32),
    ("thursday", I32),
    ("friday", I32),
    ("saturday", I32),
    ("sunday", I32),
    ("start_date", pa.date32()),
    ("end_date", pa.date32()),
    *_FEED,
)

GTFS_CALENDAR_DATES = _schema(
    ("service_id", STR),
    ("date", pa.date32()),
    ("exception_type", I32),
    *_FEED,
)

# GTFS-Realtime snapshots (our collector's compact rows; D-021). feed_date = UTC date of feed_ts.
RT_VEHICLE_POSITIONS = _schema(
    ("feed_date", pa.date32()),
    ("feed_ts", TS),
    ("vehicle_id", STR),
    ("vehicle_label", STR),
    ("trip_id", STR),
    ("route_id", STR),
    ("direction_id", I32),
    ("start_date", STR),
    ("start_time", STR),
    ("schedule_relationship", STR),
    ("latitude", F64),
    ("longitude", F64),
    ("bearing", F64),
    ("speed", F64),
    ("current_stop_sequence", I32),
    ("stop_id", STR),
    ("current_status", STR),
    ("vehicle_ts", TS),
    ("occupancy_status", STR),
)

RT_TRIP_UPDATES = _schema(
    ("feed_date", pa.date32()),
    ("feed_ts", TS),
    ("trip_id", STR),
    ("route_id", STR),
    ("direction_id", I32),
    ("start_date", STR),
    ("start_time", STR),
    ("schedule_relationship", STR),
    ("vehicle_id", STR),
    ("stop_updates", I32),
    ("next_stop_sequence", I32),
    ("next_stop_id", STR),
    ("next_arrival_ts", TS),
    ("next_departure_ts", TS),
)

SILVER_TABLES: dict[str, pa.Schema] = {
    "subway_events": SUBWAY_EVENTS,
    "alerts": ALERTS,
    "weather_hourly": WEATHER_HOURLY,
    "facilities": FACILITIES,
    "gtfs_routes": GTFS_ROUTES,
    "gtfs_stops": GTFS_STOPS,
    "gtfs_trips": GTFS_TRIPS,
    "gtfs_stop_times": GTFS_STOP_TIMES,
    "gtfs_calendar": GTFS_CALENDAR,
    "gtfs_calendar_dates": GTFS_CALENDAR_DATES,
    "rt_vehicle_positions": RT_VEHICLE_POSITIONS,
    "rt_trip_updates": RT_TRIP_UPDATES,
}

# ---------- bronze contracts (what upstream files must contain) ----------
INTEGER, STRING, BOOL, TIMESTAMP = "integer", "string", "bool", "timestamp"

BRONZE_LAMP_SUBWAY = {
    "service_date": INTEGER,
    "route_id": STRING,
    "trunk_route_id": STRING,
    "branch_route_id": STRING,
    "direction_id": BOOL,
    "direction": STRING,
    "direction_destination": STRING,
    "trip_id": STRING,
    "vehicle_id": STRING,
    "vehicle_label": STRING,
    "stop_id": STRING,
    "parent_station": STRING,
    "stop_sequence": INTEGER,
    "stop_count": INTEGER,
    "move_timestamp": INTEGER,
    "stop_timestamp": INTEGER,
    "travel_time_seconds": INTEGER,
    "dwell_time_seconds": INTEGER,
    "headway_trunk_seconds": INTEGER,
    "headway_branch_seconds": INTEGER,
    "start_time": INTEGER,
    "scheduled_arrival_time": INTEGER,
    "scheduled_departure_time": INTEGER,
    "scheduled_travel_time": INTEGER,
    "scheduled_headway_trunk": INTEGER,
    "scheduled_headway_branch": INTEGER,
}

BRONZE_LAMP_ALERTS = {
    "id": INTEGER,
    "cause": STRING,
    "cause_detail": STRING,
    "effect": STRING,
    "effect_detail": STRING,
    "severity_level": STRING,
    "severity": INTEGER,
    "alert_lifecycle": STRING,
    "duration_certainty": STRING,
    "header_text.translation.text": STRING,
    "created_timestamp": INTEGER,
    "last_modified_timestamp": INTEGER,
    "closed_timestamp": INTEGER,
    "active_period.start_timestamp": INTEGER,
    "active_period.end_timestamp": INTEGER,
    "informed_entity.route_id": STRING,
    "informed_entity.route_type": INTEGER,
    "informed_entity.direction_id": INTEGER,
    "informed_entity.stop_id": STRING,
    "informed_entity.facility_id": STRING,
    "informed_entity.activities": STRING,
}


# Bronze parquet written by our own collector (realtime/feeds.py): same columns minus lineage.
BRONZE_RT_VEHICLE_POSITIONS = {
    "feed_ts": TIMESTAMP,
    "vehicle_id": STRING,
    "trip_id": STRING,
    "latitude": "double",
    "longitude": "double",
}
BRONZE_RT_TRIP_UPDATES = {
    "feed_ts": TIMESTAMP,
    "trip_id": STRING,
    "start_date": STRING,
    "schedule_relationship": STRING,
    "next_stop_sequence": INTEGER,
}


class ContractError(Exception):
    """Raised when data does not match its contract (schema drift)."""


def _family(t: pa.DataType) -> str:
    if pa.types.is_integer(t):
        return INTEGER
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return STRING
    if pa.types.is_boolean(t):
        return BOOL
    if pa.types.is_timestamp(t):
        return TIMESTAMP
    return str(t)


def check_bronze(schema: pa.Schema, contract: dict[str, str], name: str) -> None:
    """Fail if a required column is missing or changed type family (e.g. int -> string).

    Widening within a family (int16 -> int32) is allowed; new extra columns are allowed.
    """
    problems = []
    for column, family in contract.items():
        idx = schema.get_field_index(column)
        if idx < 0:
            problems.append(f"missing column {column!r}")
            continue
        actual = _family(schema.field(idx).type)
        if actual != family:
            problems.append(f"{column!r} is {schema.field(idx).type}, expected {family}")
    if problems:
        raise ContractError(f"{name}: schema drift: " + "; ".join(problems))


def same_schema(actual: pa.Schema, expected: pa.Schema) -> list[str]:
    """Differences between two schemas by name and type (string vs large_string is equal)."""

    def norm(t: pa.DataType) -> pa.DataType:
        return pa.string() if pa.types.is_large_string(t) else t

    exp = {f.name: norm(f.type) for f in expected}
    act = {f.name: norm(f.type) for f in actual}
    diffs = [f"missing {n}" for n in exp if n not in act]
    diffs += [f"unexpected {n}" for n in act if n not in exp]
    diffs += [f"{n}: {act[n]} != {exp[n]}" for n in exp if n in act and act[n] != exp[n]]
    return diffs
