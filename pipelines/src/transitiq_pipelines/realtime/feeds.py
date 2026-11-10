"""Parse MBTA GTFS-Realtime protobuf feeds into compact rows.

VehiclePositions: one row per vehicle (all fields we use).
TripUpdates: one row per trip with only its NEXT stop prediction. A full snapshot has ~27,000
stop predictions; keeping the next stop is enough to estimate when each stop was reached (the
last prediction made while it was still the next stop) at ~1/20 of the size (D-021).
"""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime

import pyarrow as pa

if sys.platform == "win32":
    # Windows Smart App Control has blocked compiled extensions on the dev laptop (SQLAlchemy,
    # Phase 3); protobuf's pure-Python implementation is slower but needs no DLL.
    os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python")

from google.transit import gtfs_realtime_pb2 as pb  # noqa: E402

VP_URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
TU_URL = "https://cdn.mbta.com/realtime/TripUpdates.pb"

TS = pa.timestamp("us", tz="UTC")

VP_SCHEMA = pa.schema(
    [
        ("feed_ts", TS),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int32()),
        ("start_date", pa.string()),
        ("start_time", pa.string()),
        ("schedule_relationship", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("bearing", pa.float64()),
        ("speed", pa.float64()),
        ("current_stop_sequence", pa.int32()),
        ("stop_id", pa.string()),
        ("current_status", pa.string()),
        ("vehicle_ts", TS),
        ("occupancy_status", pa.string()),
    ]
)

TU_SCHEMA = pa.schema(
    [
        ("feed_ts", TS),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int32()),
        ("start_date", pa.string()),
        ("start_time", pa.string()),
        ("schedule_relationship", pa.string()),
        ("vehicle_id", pa.string()),
        ("stop_updates", pa.int32()),
        ("next_stop_sequence", pa.int32()),
        ("next_stop_id", pa.string()),
        ("next_arrival_ts", TS),
        ("next_departure_ts", TS),
    ]
)

_REL = pb.TripDescriptor.ScheduleRelationship.Name
_STATUS = pb.VehiclePosition.VehicleStopStatus.Name
_OCC = pb.VehiclePosition.OccupancyStatus.Name


def _ts(seconds: int | None) -> datetime | None:
    return datetime.fromtimestamp(seconds, UTC) if seconds else None


def _str(msg, field: str) -> str | None:
    return getattr(msg, field) if msg.HasField(field) else None


def _parse(data: bytes) -> pb.FeedMessage:
    feed = pb.FeedMessage()
    feed.ParseFromString(data)
    return feed


def parse_vehicle_positions(data: bytes) -> tuple[datetime, list[dict]]:
    feed = _parse(data)
    feed_ts = _ts(feed.header.timestamp)
    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        trip = v.trip
        pos = v.position
        rows.append(
            {
                "feed_ts": feed_ts,
                "vehicle_id": v.vehicle.id or None,
                "vehicle_label": v.vehicle.label or None,
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                "start_date": trip.start_date or None,
                "start_time": trip.start_time or None,
                "schedule_relationship": _REL(trip.schedule_relationship),
                "latitude": pos.latitude if v.HasField("position") else None,
                "longitude": pos.longitude if v.HasField("position") else None,
                "bearing": pos.bearing if pos.HasField("bearing") else None,
                "speed": pos.speed if pos.HasField("speed") else None,
                "current_stop_sequence": v.current_stop_sequence
                if v.HasField("current_stop_sequence")
                else None,
                "stop_id": _str(v, "stop_id"),
                "current_status": _STATUS(v.current_status),
                "vehicle_ts": _ts(v.timestamp),
                "occupancy_status": _OCC(v.occupancy_status)
                if v.HasField("occupancy_status")
                else None,
            }
        )
    return feed_ts, rows


def parse_trip_updates(data: bytes) -> tuple[datetime, list[dict]]:
    feed = _parse(data)
    feed_ts = _ts(feed.header.timestamp)
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        t = entity.trip_update
        trip = t.trip
        updates = sorted(t.stop_time_update, key=lambda u: u.stop_sequence)
        nxt = updates[0] if updates else None
        rows.append(
            {
                "feed_ts": feed_ts,
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                "start_date": trip.start_date or None,
                "start_time": trip.start_time or None,
                "schedule_relationship": _REL(trip.schedule_relationship),
                "vehicle_id": t.vehicle.id or None,
                "stop_updates": len(updates),
                "next_stop_sequence": nxt.stop_sequence if nxt else None,
                "next_stop_id": nxt.stop_id if nxt and nxt.stop_id else None,
                "next_arrival_ts": _ts(nxt.arrival.time)
                if nxt and nxt.HasField("arrival")
                else None,
                "next_departure_ts": _ts(nxt.departure.time)
                if nxt and nxt.HasField("departure")
                else None,
            }
        )
    return feed_ts, rows


def to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)
