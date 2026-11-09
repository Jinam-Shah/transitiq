"""One builder per silver table: pick bronze inputs, validate, write Iceberg + quarantine.

Rules per table are written as SQL CASE expressions so they read like a data contract.
Each builder records per-partition counts in data/checkpoints/silver_<table>.json, which is
what makes re-runs skip unchanged inputs and what the quality report adds up.
"""

from __future__ import annotations

import json
import logging
import re
import tempfile
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from transitiq_pipelines.checkpoint import Checkpoint
from transitiq_pipelines.silver import contracts as C
from transitiq_pipelines.silver.engine import SplitResult, concat, connect, split
from transitiq_pipelines.storage import LocalBronzeStore

if TYPE_CHECKING:  # PyIceberg is only imported where tables are written (Linux/Docker, D-012)
    from transitiq_pipelines.silver.warehouse import Warehouse

log = logging.getLogger(__name__)


# ---------- shared plumbing ----------
@dataclass
class SilverContext:
    store: LocalBronzeStore
    warehouse: Warehouse
    checkpoint_dir: Path
    force: bool = False
    now: datetime = field(default_factory=lambda: datetime.now(UTC))

    def checkpoint(self, table: str) -> Checkpoint:
        return Checkpoint(self.checkpoint_dir, f"silver_{table}")


@dataclass
class TableStats:
    table: str
    partitions_loaded: int = 0
    partitions_skipped: int = 0
    rows_in: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0
    reasons: Counter = field(default_factory=Counter)
    failures: list[dict] = field(default_factory=list)
    duration_seconds: float = 0.0

    def add(self, result: SplitResult) -> None:
        self.rows_in += result.rows_in
        self.rows_valid += result.valid.num_rows
        self.rows_quarantined += result.rejected.num_rows
        self.reasons.update(result.reasons)

    @property
    def ok(self) -> bool:
        return not self.failures

    def to_dict(self) -> dict:
        rate = self.rows_quarantined / self.rows_in if self.rows_in else 0.0
        return {
            "table": self.table,
            "partitions_loaded": self.partitions_loaded,
            "partitions_skipped": self.partitions_skipped,
            "rows_in": self.rows_in,
            "rows_valid": self.rows_valid,
            "rows_quarantined": self.rows_quarantined,
            "quarantine_rate": round(rate, 6),
            "reasons": dict(self.reasons.most_common()),
            "failures": self.failures,
            "duration_seconds": self.duration_seconds,
        }


def bronze_metas(store: LocalBronzeStore, source: str) -> list[dict]:
    return list(store.iter_meta(prefix=f"source={source}"))


def _version(meta: dict) -> tuple:
    return (meta.get("upstream_last_modified") or "", meta.get("fetched_at") or "", meta["key"])


def latest_by_partition(metas: list[dict], part: str) -> dict[str, dict]:
    pattern = re.compile(rf"/{part}=([^/]+)/")
    best: dict[str, dict] = {}
    for meta in metas:
        m = pattern.search("/" + meta["key"])
        if m and (m.group(1) not in best or _version(meta) > _version(best[m.group(1)])):
            best[m.group(1)] = meta
    return dict(sorted(best.items()))


def latest(metas: list[dict]) -> dict | None:
    return max(metas, key=_version) if metas else None


def _mark(ckpt: Checkpoint, partition: str, signature: str, result: SplitResult) -> None:
    ckpt.mark(
        partition,
        signature=signature,
        rows_in=result.rows_in,
        valid=result.valid.num_rows,
        quarantined=result.rejected.num_rows,
        reasons=dict(result.reasons),
    )


def _unchanged(ctx: SilverContext, ckpt: Checkpoint, partition: str, signature: str) -> bool:
    prev = ckpt.get(partition)
    return not ctx.force and prev is not None and prev.get("signature") == signature


def _sql_literal(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


# ---------- subway_events (LAMP, partitioned by service_date) ----------
SUBWAY_SQL = """
WITH src AS (
    SELECT *, row_number() OVER () AS _row_number FROM read_parquet(
        {path}, hive_partitioning = false
    )
), typed AS (
    SELECT
        try_strptime(CAST(service_date AS VARCHAR), '%Y%m%d')::DATE AS service_date,
        route_id, trunk_route_id, branch_route_id,
        CAST(direction_id AS INTEGER) AS direction_id, direction, direction_destination,
        trip_id, vehicle_id, vehicle_label, stop_id, parent_station,
        CAST(stop_sequence AS INTEGER) AS stop_sequence,
        CAST(stop_count AS INTEGER) AS stop_count,
        to_timestamp(move_timestamp) AS move_ts,
        to_timestamp(stop_timestamp) AS stop_ts,
        CAST(travel_time_seconds AS BIGINT) AS travel_time_seconds,
        CAST(dwell_time_seconds AS BIGINT) AS dwell_time_seconds,
        CAST(headway_trunk_seconds AS BIGINT) AS headway_trunk_seconds,
        CAST(headway_branch_seconds AS BIGINT) AS headway_branch_seconds,
        CAST(start_time AS BIGINT) AS start_time,
        CAST(scheduled_arrival_time AS BIGINT) AS scheduled_arrival_time,
        CAST(scheduled_departure_time AS BIGINT) AS scheduled_departure_time,
        CAST(scheduled_travel_time AS BIGINT) AS scheduled_travel_time,
        CAST(scheduled_headway_trunk AS BIGINT) AS scheduled_headway_trunk,
        CAST(scheduled_headway_branch AS BIGINT) AS scheduled_headway_branch,
        _row_number
    FROM src
), flagged AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY trip_id, stop_id, stop_sequence, vehicle_id ORDER BY _row_number
        ) AS _dup,
        CASE
            WHEN service_date IS NULL THEN 'missing_field:service_date'
            WHEN service_date <> DATE {expected} THEN 'service_date_mismatch'
            WHEN coalesce(route_id, '') = '' THEN 'missing_field:route_id'
            WHEN coalesce(trip_id, '') = '' THEN 'missing_field:trip_id'
            WHEN coalesce(stop_id, '') = '' THEN 'missing_field:stop_id'
            WHEN move_ts IS NULL AND stop_ts IS NULL THEN 'no_timestamps'
            WHEN travel_time_seconds < 0 OR dwell_time_seconds < 0
              OR headway_trunk_seconds < 0 OR headway_branch_seconds < 0 THEN 'negative_duration'
            WHEN travel_time_seconds > 10800 THEN 'implausible_travel_time'
            WHEN dwell_time_seconds > 7200 THEN 'implausible_dwell_time'
            WHEN coalesce(stop_ts, move_ts) < CAST(service_date AS TIMESTAMPTZ) - INTERVAL 1 DAY
              OR coalesce(stop_ts, move_ts) > CAST(service_date AS TIMESTAMPTZ) + INTERVAL 2 DAY
              THEN 'timestamp_outside_service_day'
        END AS _rule
    FROM typed
)
SELECT * EXCLUDE (_dup, _rule),
    coalesce(_rule, CASE WHEN _dup > 1 THEN 'duplicate_event' END) AS _reason
FROM flagged
"""


def build_subway_events(ctx: SilverContext) -> TableStats:
    name, contract = "subway_events", C.SUBWAY_EVENTS
    stats, t0 = TableStats(name), time.monotonic()
    ckpt = ctx.checkpoint(name)
    inputs = latest_by_partition(bronze_metas(ctx.store, "lamp_subway_otp"), "service_date")
    pending = {}
    for day, meta in inputs.items():
        if _unchanged(ctx, ckpt, day, meta["key"]):
            stats.partitions_skipped += 1
        else:
            pending[day] = meta

    by_month: dict[str, list[str]] = defaultdict(list)
    for day in pending:
        by_month[day[:7]].append(day)

    conn = connect()
    for month, days in sorted(by_month.items()):
        results: dict[str, SplitResult] = {}
        for day in days:
            meta = pending[day]
            path = ctx.store.path(meta["key"])
            try:
                C.check_bronze(pq.read_schema(path), C.BRONZE_LAMP_SUBWAY, meta["key"])
                sql = SUBWAY_SQL.format(path=_sql_literal(path), expected=f"'{day}'")
                results[day] = split(conn, sql, contract, meta["key"], meta.get("sha256"), ctx.now)
            except Exception as exc:
                log.error("subway_events %s failed: %s", day, exc)
                stats.failures.append({"partition": day, "error": str(exc)[:500]})
        if not results:
            continue
        merged = concat(list(results.values()), contract)
        ctx.warehouse.replace_partitions(
            name, contract, "service_date", list(results), merged.valid
        )
        for day, result in results.items():
            ctx.warehouse.write_quarantine(name, f"service_date={day}", result.rejected)
            _mark(ckpt, day, pending[day]["key"], result)
            stats.add(result)
            stats.partitions_loaded += 1
        log.info(
            "subway_events %s: %d days, %d valid rows", month, len(results), merged.valid.num_rows
        )
    stats.duration_seconds = round(time.monotonic() - t0, 2)
    return stats


# ---------- alerts (LAMP alerts history, replaced whole) ----------
ALERTS_SQL = """
WITH src AS (
    SELECT *, row_number() OVER () AS _row_number FROM read_parquet(
        {path}, hive_partitioning = false
    )
), typed AS (
    SELECT
        CAST(id AS BIGINT) AS alert_id, cause, cause_detail, effect, effect_detail,
        severity_level, CAST(severity AS INTEGER) AS severity, alert_lifecycle,
        duration_certainty, "header_text.translation.text" AS header_text,
        to_timestamp(created_timestamp) AS created_ts,
        to_timestamp(last_modified_timestamp) AS last_modified_ts,
        to_timestamp(closed_timestamp) AS closed_ts,
        to_timestamp("active_period.start_timestamp") AS active_start_ts,
        to_timestamp("active_period.end_timestamp") AS active_end_ts,
        "informed_entity.route_id" AS route_id,
        CAST("informed_entity.route_type" AS INTEGER) AS route_type,
        CAST("informed_entity.direction_id" AS INTEGER) AS direction_id,
        "informed_entity.stop_id" AS stop_id,
        "informed_entity.facility_id" AS facility_id,
        "informed_entity.activities" AS activities,
        _row_number,
        -- raw keys for duplicate detection (no hash(): a UBIGINT hash column triggered an
        -- intermittent DuckDB INTERNAL cast error on the 5.2M-row file in Docker)
        id AS _k_id, last_modified_timestamp AS _k_mod,
        "active_period.start_timestamp" AS _k_start, "active_period.end_timestamp" AS _k_end
    FROM src
), flagged AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY _k_id, _k_mod, _k_start, _k_end, route_id, route_type, direction_id,
                stop_id, facility_id, activities
            ORDER BY _row_number
        ) AS _dup,
        CASE
            WHEN alert_id IS NULL THEN 'missing_field:alert_id'
            WHEN coalesce(effect, '') = '' THEN 'missing_field:effect'
            WHEN last_modified_ts IS NULL THEN 'missing_field:last_modified_ts'
            -- active_start_ts may legitimately be NULL upstream (~22% of rows); not rejected
            WHEN active_end_ts < active_start_ts THEN 'end_before_start'
        END AS _rule
    FROM typed
)
SELECT * EXCLUDE (_k_id, _k_mod, _k_start, _k_end, _dup, _rule),
    coalesce(_rule, CASE WHEN _dup > 1 THEN 'duplicate_row' END) AS _reason
FROM flagged
"""


def build_alerts(ctx: SilverContext) -> TableStats:
    name, contract = "alerts", C.ALERTS
    stats, t0 = TableStats(name), time.monotonic()
    ckpt = ctx.checkpoint(name)
    meta = latest(bronze_metas(ctx.store, "lamp_alerts"))
    if meta is None:
        stats.failures.append({"partition": "all", "error": "no lamp_alerts bronze file"})
    elif _unchanged(ctx, ckpt, "all", meta["key"]):
        stats.partitions_skipped += 1
    else:
        try:
            path = ctx.store.path(meta["key"])
            C.check_bronze(pq.read_schema(path), C.BRONZE_LAMP_ALERTS, meta["key"])
            result = split(
                connect(),
                ALERTS_SQL.format(path=_sql_literal(path)),
                contract,
                meta["key"],
                meta.get("sha256"),
                ctx.now,
            )
            ctx.warehouse.replace_all(name, contract, result.valid)
            ctx.warehouse.write_quarantine(name, "all", result.rejected)
            _mark(ckpt, "all", meta["key"], result)
            stats.add(result)
            stats.partitions_loaded += 1
        except Exception as exc:
            stats.failures.append({"partition": "all", "error": str(exc)[:500]})
    stats.duration_seconds = round(time.monotonic() - t0, 2)
    return stats


# ---------- weather_hourly (Open-Meteo months, replaced whole) ----------
WEATHER_VARS = {
    "temperature_2m": "temperature_2m_c",
    "relative_humidity_2m": "relative_humidity_2m_pct",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "snowfall": "snowfall_cm",
    "snow_depth": "snow_depth_m",
    "weather_code": "weather_code",
    "wind_speed_10m": "wind_speed_10m_kmh",
    "wind_gusts_10m": "wind_gusts_10m_kmh",
}

WEATHER_SQL = """
SELECT * EXCLUDE (_dup, _rule),
    coalesce(_rule, CASE WHEN _dup > 1 THEN 'duplicate_hour' END) AS _reason
FROM (
    SELECT *,
        row_number() OVER (PARTITION BY hour_ts ORDER BY _row_number) AS _dup,
        CASE
            WHEN hour_ts IS NULL THEN 'missing_field:hour_ts'
            WHEN temperature_2m_c IS NULL THEN 'missing_field:temperature_2m'
            WHEN temperature_2m_c < -40 OR temperature_2m_c > 50 THEN 'implausible_temperature'
            WHEN relative_humidity_2m_pct < 0 OR relative_humidity_2m_pct > 100
              THEN 'implausible_humidity'
            WHEN precipitation_mm < 0 OR rain_mm < 0 OR snowfall_cm < 0
              THEN 'negative_precipitation'
        END AS _rule
    FROM wx
)
"""


def _weather_rank(meta: dict) -> tuple:
    return (bool(meta.get("complete")), meta.get("fetched_at") or "", meta["key"])


def weather_month_files(metas: list[dict]) -> dict[str, list[dict]]:
    """All files per month, best first: the complete file, then newer partial fetches."""
    months: dict[str, list[dict]] = defaultdict(list)
    for meta in metas:
        if meta.get("month"):
            months[meta["month"]].append(meta)
    return {
        m: sorted(files, key=_weather_rank, reverse=True) for m, files in sorted(months.items())
    }


def merge_month(tables: list[pa.Table]) -> list[pa.Table]:
    """Pick each hour from the best file that has a temperature for it (else the best file).

    Partial fetches overlap: an early fetch covers the whole month with recent days still
    empty, a later one may cover only the last days. Taking whole files would lose hours
    (a real bug: a 6-day partial once replaced a full-month partial). Returns one filtered
    table per input file so every row keeps its own bronze file as lineage.
    """
    chosen: dict = {}
    for i, table in enumerate(tables):
        for ts, temp in zip(
            table.column("hour_ts").to_pylist(),
            table.column("temperature_2m_c").to_pylist(),
            strict=True,
        ):
            if ts is None:
                continue
            if ts not in chosen or (chosen[ts][1] is None and temp is not None):
                chosen[ts] = (i, temp)
    out = []
    for i, table in enumerate(tables):
        keep = [chosen.get(ts, (None,))[0] == i for ts in table.column("hour_ts").to_pylist()]
        out.append(table.filter(pa.array(keep)))
    return out


def weather_table(payload: dict) -> pa.Table:
    hourly = payload["hourly"]
    times = [datetime.fromisoformat(t).replace(tzinfo=UTC) if t else None for t in hourly["time"]]
    cols = {"hour_ts": pa.array(times, pa.timestamp("us", tz="UTC"))}
    for src, dest in WEATHER_VARS.items():
        values = hourly.get(src, [None] * len(times))
        typ = pa.int32() if dest == "weather_code" else pa.float64()
        cols[dest] = pa.array(values, typ)
    cols["_row_number"] = pa.array(range(1, len(times) + 1), pa.int64())
    return pa.table(cols)


def build_weather_hourly(ctx: SilverContext) -> TableStats:
    name, contract = "weather_hourly", C.WEATHER_HOURLY
    stats, t0 = TableStats(name), time.monotonic()
    ckpt = ctx.checkpoint(name)
    inputs = weather_month_files(bronze_metas(ctx.store, "open_meteo_hourly"))
    signature = "|".join(m["key"] for files in inputs.values() for m in files)
    if not inputs:
        stats.failures.append({"partition": "all", "error": "no open_meteo_hourly bronze files"})
    elif _unchanged(ctx, ckpt, "all", signature):
        stats.partitions_skipped += 1
    else:
        conn = connect()
        results = []
        try:
            for files in inputs.values():
                tables = [
                    weather_table(json.loads(ctx.store.path(m["key"]).read_text("utf-8")))
                    for m in files
                ]
                for meta, wx in zip(files, merge_month(tables), strict=True):
                    if wx.num_rows == 0:
                        continue
                    conn.register("wx", wx)
                    results.append(split(conn, WEATHER_SQL, contract, meta["key"], None, ctx.now))
                    conn.unregister("wx")
            merged = concat(results, contract)
            ctx.warehouse.replace_all(name, contract, merged.valid)
            ctx.warehouse.clear_quarantine(name)
            ctx.warehouse.write_quarantine(name, "all", merged.rejected)
            _mark(ckpt, "all", signature, merged)
            stats.add(merged)
            stats.partitions_loaded += len(inputs)
        except Exception as exc:
            stats.failures.append({"partition": "all", "error": str(exc)[:500]})
    stats.duration_seconds = round(time.monotonic() - t0, 2)
    return stats


# ---------- facilities (MBTA V3 snapshot, replaced whole) ----------
FACILITIES_SQL = """
SELECT * EXCLUDE (_dup, _rule),
    coalesce(_rule, CASE WHEN _dup > 1 THEN 'duplicate_key' END) AS _reason
FROM (
    SELECT *,
        row_number() OVER (PARTITION BY facility_id ORDER BY _row_number) AS _dup,
        CASE
            WHEN coalesce(facility_id, '') = '' THEN 'missing_field:facility_id'
            WHEN coalesce(facility_type, '') = '' THEN 'missing_field:facility_type'
            WHEN latitude NOT BETWEEN 41.0 AND 43.5 OR longitude NOT BETWEEN -72.5 AND -69.5
              THEN 'coordinates_out_of_range'
        END AS _rule
    FROM fac
)
"""


def facilities_table(payload: dict) -> pa.Table:
    rows = []
    for i, item in enumerate(payload.get("data", []), start=1):
        attrs = item.get("attributes") or {}
        stop = ((item.get("relationships") or {}).get("stop") or {}).get("data") or {}
        rows.append(
            {
                "facility_id": item.get("id"),
                "facility_type": attrs.get("type"),
                "long_name": attrs.get("long_name"),
                "short_name": attrs.get("short_name"),
                "stop_id": stop.get("id"),
                "latitude": attrs.get("latitude"),
                "longitude": attrs.get("longitude"),
                "_row_number": i,
            }
        )
    schema = pa.schema(
        [f for f in C.FACILITIES if not f.name.startswith("_")]
        + [pa.field("_row_number", pa.int64())]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def build_facilities(ctx: SilverContext) -> TableStats:
    name, contract = "facilities", C.FACILITIES
    stats, t0 = TableStats(name), time.monotonic()
    ckpt = ctx.checkpoint(name)
    meta = latest(bronze_metas(ctx.store, "mbta_v3_facilities"))
    if meta is None:
        stats.failures.append({"partition": "all", "error": "no mbta_v3_facilities bronze file"})
    elif _unchanged(ctx, ckpt, "all", meta["key"]):
        stats.partitions_skipped += 1
    else:
        try:
            payload = json.loads(ctx.store.path(meta["key"]).read_text("utf-8"))
            conn = connect()
            fac = facilities_table(payload)  # noqa: F841 (referenced by name in SQL)
            conn.register("fac", fac)
            result = split(conn, FACILITIES_SQL, contract, meta["key"], None, ctx.now)
            ctx.warehouse.replace_all(name, contract, result.valid)
            ctx.warehouse.write_quarantine(name, "all", result.rejected)
            _mark(ckpt, "all", meta["key"], result)
            stats.add(result)
            stats.partitions_loaded += 1
        except Exception as exc:
            stats.failures.append({"partition": "all", "error": str(exc)[:500]})
    stats.duration_seconds = round(time.monotonic() - t0, 2)
    return stats


# ---------- GTFS static (latest feed, each table replaced whole) ----------
@dataclass(frozen=True)
class GtfsSpec:
    table: str
    file: str
    contract: pa.Schema
    keys: tuple[str, ...]
    required: tuple[str, ...]


GTFS_SPECS = (
    GtfsSpec("gtfs_routes", "routes.txt", C.GTFS_ROUTES, ("route_id",), ("route_id", "route_type")),
    GtfsSpec("gtfs_stops", "stops.txt", C.GTFS_STOPS, ("stop_id",), ("stop_id", "stop_name")),
    GtfsSpec(
        "gtfs_trips", "trips.txt", C.GTFS_TRIPS, ("trip_id",), ("trip_id", "route_id", "service_id")
    ),
    GtfsSpec(
        "gtfs_stop_times",
        "stop_times.txt",
        C.GTFS_STOP_TIMES,
        ("trip_id", "stop_sequence"),
        ("trip_id", "stop_sequence", "stop_id"),
    ),
    GtfsSpec(
        "gtfs_calendar",
        "calendar.txt",
        C.GTFS_CALENDAR,
        ("service_id",),
        ("service_id", "start_date", "end_date"),
    ),
    GtfsSpec(
        "gtfs_calendar_dates",
        "calendar_dates.txt",
        C.GTFS_CALENDAR_DATES,
        ("service_id", "date"),
        ("service_id", "date", "exception_type"),
    ),
)

_TIME_RE = r"^[0-9]{1,3}:[0-5][0-9]:[0-5][0-9]$"
_SQL_TYPES = {
    pa.string(): "VARCHAR",
    pa.int32(): "INTEGER",
    pa.float64(): "DOUBLE",
    pa.date32(): "DATE",
}
# Explicit CSV dialect: DuckDB's sniffer mis-detected quoting on MBTA trips.txt (a quoted
# headsign containing commas), so nothing is left to guessing.
CSV_OPTS = (
    "all_varchar = true, header = true, delim = ',', quote = '\"', escape = '\"', "
    "hive_partitioning = false"
)


def _time_seconds(col: str) -> str:
    t = f"nullif(trim(\"{col}\"), '')"
    return (
        f"CASE WHEN regexp_matches({t}, '{_TIME_RE}') THEN "
        f"CAST(split_part({t}, ':', 1) AS INTEGER) * 3600 + "
        f"CAST(split_part({t}, ':', 2) AS INTEGER) * 60 + "
        f"CAST(split_part({t}, ':', 3) AS INTEGER) END"
    )


def gtfs_sql(spec: GtfsSpec, path: Path, header: list[str]) -> str:
    """Build the typed SELECT + rule CASE for one GTFS file from its contract."""
    selects, rules = [], []
    present = set(header)
    for f in spec.contract:
        name = f.name
        if name.startswith("_"):
            continue
        if name == "feed_version":
            selects.append("CAST($feed_version AS VARCHAR) AS feed_version")
            continue
        if name in ("arrival_seconds", "departure_seconds"):
            src = name.replace("_seconds", "_time")
            expr = _time_seconds(src) if src in present else "CAST(NULL AS INTEGER)"
            selects.append(f"{expr} AS {name}")
            if src in present:
                rules.append(
                    f"WHEN nullif(trim(\"{src}\"), '') IS NOT NULL "
                    f"AND {_time_seconds(src)} IS NULL THEN 'invalid_time:{src}'"
                )
            continue
        if name not in present:  # optional column absent from this feed: typed NULL
            selects.append(f'CAST(NULL AS {_SQL_TYPES[f.type]}) AS "{name}"')
            if name in spec.required:
                rules.insert(0, f"WHEN TRUE THEN 'missing_column:{name}'")
            continue
        raw = f"nullif(trim(\"{name}\"), '')"
        if f.type == pa.string():
            expr = raw
        elif f.type == pa.date32():
            expr = f"try_strptime({raw}, '%Y%m%d')::DATE"
        elif f.type == pa.float64():
            expr = f"TRY_CAST({raw} AS DOUBLE)"
        else:
            expr = f"TRY_CAST({raw} AS INTEGER)"
        selects.append(f'{expr} AS "{name}"')
        if name in spec.required:
            rules.insert(0, f"WHEN {raw} IS NULL THEN 'missing_field:{name}'")
        if f.type != pa.string() and name in present:
            rules.append(f"WHEN {raw} IS NOT NULL AND {expr} IS NULL THEN 'invalid_type:{name}'")
    keys = ", ".join(f'"{k}"' for k in spec.keys)
    rule_sql = "CASE " + " ".join(rules) + " END" if rules else "NULL"
    # Identifiers below come from our own contracts, not from user input.
    return f"""
    WITH src AS (
        SELECT *, row_number() OVER () AS _row_number
        FROM read_csv({_sql_literal(path)}, {CSV_OPTS})
    ), typed AS (
        SELECT {", ".join(selects)}, {rule_sql} AS _rule, _row_number FROM src
    )
    SELECT * EXCLUDE (_dup, _rule),
        coalesce(_rule, CASE WHEN _dup > 1 THEN 'duplicate_key' END) AS _reason
    FROM (
        SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY _row_number) AS _dup
        FROM typed
    )
    """  # noqa: S608


def build_gtfs(ctx: SilverContext) -> list[TableStats]:
    meta = latest(bronze_metas(ctx.store, "gtfs_static"))
    out = []
    if meta is None:
        for spec in GTFS_SPECS:
            s = TableStats(spec.table)
            s.failures.append({"partition": "all", "error": "no gtfs_static bronze file"})
            out.append(s)
        return out
    with tempfile.TemporaryDirectory() as tmp:
        extracted = False
        conn = connect()
        for spec in GTFS_SPECS:
            stats, t0 = TableStats(spec.table), time.monotonic()
            ckpt = ctx.checkpoint(spec.table)
            if _unchanged(ctx, ckpt, "all", meta["key"]):
                stats.partitions_skipped += 1
                out.append(stats)
                continue
            try:
                if not extracted:
                    with zipfile.ZipFile(ctx.store.path(meta["key"])) as zf:
                        zf.extractall(tmp)  # noqa: S202 (MBTA feed, flat .txt files)
                    extracted = True
                path = Path(tmp) / spec.file
                header = [
                    c[0]
                    for c in conn.execute(
                        f"DESCRIBE SELECT * FROM read_csv({_sql_literal(path)}, {CSV_OPTS})"  # noqa: E501, S608
                    ).fetchall()
                ]
                conn.execute("SET VARIABLE feed_version = ?", [meta.get("feed_version") or ""])
                sql = gtfs_sql(spec, path, header).replace(
                    "$feed_version", "getvariable('feed_version')"
                )
                result = split(conn, sql, spec.contract, meta["key"], meta.get("sha256"), ctx.now)
                ctx.warehouse.replace_all(spec.table, spec.contract, result.valid)
                ctx.warehouse.write_quarantine(spec.table, "all", result.rejected)
                _mark(ckpt, "all", meta["key"], result)
                stats.add(result)
                stats.partitions_loaded += 1
            except Exception as exc:
                stats.failures.append({"partition": "all", "error": str(exc)[:500]})
            stats.duration_seconds = round(time.monotonic() - t0, 2)
            out.append(stats)
    return out


TABLE_GROUPS = {
    "subway_events": lambda ctx: [build_subway_events(ctx)],
    "alerts": lambda ctx: [build_alerts(ctx)],
    "weather_hourly": lambda ctx: [build_weather_hourly(ctx)],
    "facilities": lambda ctx: [build_facilities(ctx)],
    "gtfs": build_gtfs,
}
