"""Training rows for the Leave-By Planner: "a rider reaches station O at time t, going to D".

Each row is one simulated rider on a real service day (D-027):

    t        a random moment 0-20 min before a real departure from O (riders come when trains run)
    target   seconds from t until the rider ARRIVES at D on the first train that leaves O at or
             after t and really reached D (any branch of the line, e.g. any Green Line branch
             between Park Street and Kenmore). This is wait + ride, measured, not modelled.

Features are things the planner can know at time t: the line, both stations, the schedule
(scheduled ride time and scheduled headway), the calendar, the weather of that hour, MBTA alerts
already active on the line, and how late the line ran in the hour before t. Nothing measured
after t is used.

Rows are built one month at a time (so memory stays bounded) and written to
data/ml/dataset/month=YYYY-MM.parquet.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import duckdb

from transitiq_pipelines import fsutil

log = logging.getLogger(__name__)

SUBWAY_TRUNKS = ("Red", "Mattapan", "Orange", "Blue", "Green")
MAX_LEAD_SECONDS = 1200  # rider arrives 0-20 min before the reference departure
MAX_JOURNEY_SECONDS = 3 * 3600  # longer is a data error (vehicle id reused, day boundary)
# Alerts that change service (LAMP effect_detail). Elevator/escalator, parking, bike and notice
# alerts are ~half of all subway alerts and say nothing about travel time.
SERVICE_EFFECTS = (
    "DELAY",
    "SHUTTLE",
    "SUSPENSION",
    "STATION_CLOSURE",
    "STOP_CLOSURE",
    "CANCELLATION",
    "SERVICE_CHANGE",
    "SCHEDULE_CHANGE",
    "DETOUR",
)

# One query per month. {lo}/{hi} bound service_date; {n} = riders per line, direction and day;
# {seed} makes the sample reproducible.
SQL = """
with seg as (
    select
        service_date, trunk_route_id as line, route_id, direction_id, trip_id, vehicle_id,
        from_station_id, to_station_id, departure_ts, arrival_ts, scheduled_seconds,
        row_number() over k as seg_no,
        sum(scheduled_seconds) over k as sched_end,
        sum(case when scheduled_seconds is null then 1 else 0 end) over k as nulls_end
    from gold.fact_travel_time
    where service_date between date '{lo}' and date '{hi}'
      and trunk_route_id in {trunks}
      and departure_ts is not null and arrival_ts is not null
    window k as (
        partition by service_date, trip_id, vehicle_id
        order by departure_ts, arrival_ts, from_station_id, to_station_id
        rows between unbounded preceding and current row
    )
),
dep as (  -- leaving a station
    select service_date, line, route_id, direction_id, trip_id, vehicle_id,
           from_station_id as station, departure_ts as ts, seg_no,
           coalesce(sched_end - scheduled_seconds, 0) as sched_before,
           nulls_end - (case when scheduled_seconds is null then 1 else 0 end) as nulls_before
    from seg
),
arr as (  -- reaching a station
    select service_date, trip_id, vehicle_id, to_station_id as station, arrival_ts as ts,
           seg_no, sched_end, nulls_end
    from seg
),
ref as (  -- reference departures, {n} per line, direction and day
    select *
    from dep
    qualify row_number() over (
        partition by service_date, line, direction_id
        order by hash(trip_id || vehicle_id || station || '{seed}'), trip_id, vehicle_id, seg_no
    ) <= {n}
),
riders as (  -- each reference departure gets a random destination later on the same trip
    select
        r.service_date, r.line, r.direction_id, r.station as origin, a.station as destination,
        r.route_id as ref_route_id,
        r.ts - to_seconds((hash(r.trip_id || r.station || a.station || '{seed}') % {lead})::bigint)
            as rider_ts
    from ref as r
    join arr as a
      on a.service_date = r.service_date and a.trip_id = r.trip_id
     and a.vehicle_id = r.vehicle_id and a.seg_no >= r.seg_no and a.station <> r.station
    qualify row_number() over (
        partition by r.service_date, r.trip_id, r.vehicle_id, r.station, r.seg_no
        order by hash(a.station || '{seed}'), a.seg_no
    ) = 1
),
pairs as (select distinct service_date, line, direction_id, origin, destination from riders),
journeys as (  -- every real trip that left O and later reached D that day
    select
        p.service_date, p.line, p.direction_id, p.origin, p.destination,
        d.route_id, d.trip_id, d.ts as dep_ts, a.ts as arr_ts,
        a.seg_no - d.seg_no + 1 as segments,
        case when a.nulls_end - d.nulls_before = 0 then a.sched_end - d.sched_before end
            as sched_ride_s
    from pairs as p
    join dep as d
      on d.service_date = p.service_date and d.line = p.line
     and d.direction_id = p.direction_id and d.station = p.origin
    join arr as a
      on a.service_date = d.service_date and a.trip_id = d.trip_id
     and a.vehicle_id = d.vehicle_id and a.station = p.destination and a.seg_no >= d.seg_no
    -- two records leaving O in the same second (duplicate vehicle records): keep one, the
    -- earliest arrival, so the ASOF join below is deterministic
    qualify row_number() over (
        partition by p.service_date, p.line, p.direction_id, p.origin, p.destination, d.ts
        order by a.ts, d.trip_id, d.vehicle_id, d.seg_no, a.seg_no
    ) = 1
),
served as (  -- how many routes (branches) serve the pair that day
    select service_date, line, direction_id, origin, destination,
           count(distinct route_id) as routes_serving
    from journeys group by all
),
taken as (  -- the first such trip leaving at or after the rider arrives
    select r.*, j.route_id, j.trip_id, j.dep_ts, j.arr_ts, j.segments, j.sched_ride_s
    from riders as r
    asof join journeys as j
      on j.service_date = r.service_date and j.line = r.line
     and j.direction_id = r.direction_id and j.origin = r.origin
     and j.destination = r.destination and r.rider_ts <= j.dep_ts
),
recent as (  -- how the line ran, per 15-minute bucket (joined for the 4 buckets before t)
    select trunk_route_id as line, direction_id, time_bucket(interval 15 minute, arrival_ts) as b,
           sum(travel_seconds) filter (where scheduled_seconds > 0) as travel,
           sum(scheduled_seconds) filter (where scheduled_seconds > 0) as sched,
           count(*) as n
    from gold.fact_travel_time
    where service_date between date '{lo}' - 1 and date '{hi}'
      and trunk_route_id in {trunks}
    group by all
),
recent_hw as (
    select trunk_route_id as line, direction_id, time_bucket(interval 15 minute, arrival_ts) as b,
           round(avg(least(headway_ratio, 10)), 9) as hw_ratio
    from gold.fact_headway
    where service_date between date '{lo}' - 1 and date '{hi}'
      and trunk_route_id in {trunks} and headway_ratio is not null
    group by all
),
hw_known as (  -- arrivals that carry a scheduled headway (one row per station and second)
    select service_date, trunk_route_id, direction_id, station_id, arrival_ts,
           min(scheduled_headway_seconds) as scheduled_headway_seconds,
           min(scheduled_headway_branch_seconds) as scheduled_headway_branch_seconds
    from gold.fact_headway
    where service_date between date '{lo}' and date '{hi}'
      and trunk_route_id in {trunks} and scheduled_headway_seconds is not null
    group by all
),
alert_versions as (  -- service alerts on a subway line, each version with its valid interval
    select
        alert_id, severity,
        effect_detail = 'DELAY' as is_delay,
        case when route like 'Green%' then 'Green' else route end as line,
        greatest(last_modified_ts, coalesce(start_ts, last_modified_ts)) as valid_from,
        least(
            coalesce(lead(last_modified_ts) over (partition by alert_id order by last_modified_ts),
                     timestamptz '9999-12-31'),
            coalesce(end_ts, timestamptz '9999-12-31'),
            coalesce(closed_ts, timestamptz '9999-12-31')
        ) as valid_to
    from gold.fact_alert, unnest(string_split(coalesce(routes, ''), ',')) as u(route)
    where route in ('Red', 'Mattapan', 'Orange', 'Blue', 'Green-B', 'Green-C', 'Green-D',
                    'Green-E')
      and effect_detail in {service_effects}
      and coalesce(end_ts, timestamptz '9999-12-31') >= timestamptz '{lo}' - interval 1 day
      and coalesce(start_ts, last_modified_ts) <= timestamptz '{hi}' + interval 2 day
)
select
    t.service_date,
    t.line,
    t.direction_id,
    t.origin,
    t.destination,
    t.ref_route_id,
    t.route_id as route_taken,
    t.rider_ts,
    date_diff('second', t.rider_ts, t.arr_ts) as journey_s,
    date_diff('second', t.rider_ts, t.dep_ts) as wait_s,
    date_diff('second', t.dep_ts, t.arr_ts) as ride_s,
    t.segments,
    t.sched_ride_s::bigint as sched_ride_s,
    -- a ride faster than possible or far slower than any disruption: a vehicle taken out of
    -- service that kept its trip id, or a timestamp error (counted, then dropped)
    date_diff('second', t.dep_ts, t.arr_ts) < 0
      or date_diff('second', t.dep_ts, t.arr_ts) > 4 * coalesce(t.sched_ride_s, 900) + 1800
        as is_outlier,
    s.routes_serving,
    -- the schedule's headway at the origin: from the last train there at or before this one,
    -- or (first train of the day) from the next one
    coalesce(hw.scheduled_headway_seconds, hwn.scheduled_headway_seconds) as sched_headway_trunk_s,
    coalesce(hw.scheduled_headway_branch_seconds, hwn.scheduled_headway_branch_seconds)
        as sched_headway_branch_s,
    extract('hour' from timezone('America/New_York', t.rider_ts))
      + extract('minute' from timezone('America/New_York', t.rider_ts)) / 60.0 as hour_local,
    dd.day_of_week,
    dd.day_type,
    dd.is_holiday::integer as is_holiday,
    w.temperature_c,
    w.precipitation_mm,
    w.snowfall_cm,
    w.snow_depth_m,
    w.wind_gusts_kmh,
    (select round(sum(travel) / nullif(sum(sched), 0), 6) from recent as rc
      where rc.line = t.line and rc.direction_id = t.direction_id
        and rc.b >= time_bucket(interval 15 minute, t.rider_ts) - interval 60 minute
        and rc.b < time_bucket(interval 15 minute, t.rider_ts)) as recent_ride_ratio,
    (select coalesce(sum(n), 0) from recent as rc
      where rc.line = t.line and rc.direction_id = t.direction_id
        and rc.b >= time_bucket(interval 15 minute, t.rider_ts) - interval 60 minute
        and rc.b < time_bucket(interval 15 minute, t.rider_ts)) as recent_segments,
    (select round(avg(hw_ratio), 6) from recent_hw as rh
      where rh.line = t.line and rh.direction_id = t.direction_id
        and rh.b >= time_bucket(interval 15 minute, t.rider_ts) - interval 60 minute
        and rh.b < time_bucket(interval 15 minute, t.rider_ts)) as recent_headway_ratio,
    (select count(distinct alert_id) filter (where is_delay) from alert_versions as av
      where av.line = t.line and t.rider_ts >= av.valid_from and t.rider_ts < av.valid_to)
        as delay_alerts,
    (select count(distinct alert_id) filter (where not is_delay) from alert_versions as av
      where av.line = t.line and t.rider_ts >= av.valid_from and t.rider_ts < av.valid_to)
        as disruption_alerts,
    (select coalesce(max(severity), 0) from alert_versions as av
      where av.line = t.line and t.rider_ts >= av.valid_from and t.rider_ts < av.valid_to)
        as alert_max_severity
from taken as t
join served as s using (service_date, line, direction_id, origin, destination)
left join gold.dim_date as dd on dd.date = t.service_date
left join gold.dim_weather_hour as w on w.hour_ts = date_trunc('hour', t.rider_ts)
asof left join hw_known as hw
  on hw.service_date = t.service_date and hw.trunk_route_id = t.line
 and hw.direction_id = t.direction_id and hw.station_id = t.origin
 and hw.arrival_ts <= t.dep_ts
asof left join hw_known as hwn
  on hwn.service_date = t.service_date and hwn.trunk_route_id = t.line
 and hwn.direction_id = t.direction_id and hwn.station_id = t.origin
 and hwn.arrival_ts > t.dep_ts
where date_diff('second', t.rider_ts, t.arr_ts) between 1 and {max_journey}
"""


def _sql_list(values: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def months_with_data(conn) -> list[tuple[date, date]]:
    rows = conn.execute(
        "select date_trunc('month', service_date)::date as m, min(service_date), max(service_date) "
        "from gold.fact_travel_time group by 1 order by 1"
    ).fetchall()
    return [(lo, hi) for _, lo, hi in rows]


def build_month(conn, lo: date, hi: date, n: int, seed: int):
    sql = SQL.format(
        lo=lo.isoformat(),
        hi=hi.isoformat(),
        n=int(n),
        seed=int(seed),
        lead=MAX_LEAD_SECONDS,
        max_journey=MAX_JOURNEY_SECONDS,
        trunks=_sql_list(SUBWAY_TRUNKS),
        service_effects=_sql_list(SERVICE_EFFECTS),
    )
    return conn.execute(sql).fetch_arrow_table()


def build_dataset(
    db_path: Path,
    out_dir: Path,
    riders_per_line_day: int = 150,
    seed: int = 7,
    memory_limit: str = "2GB",
    rebuild: bool = False,
) -> dict:
    """Write one Parquet file per month; months already written are kept unless rebuild."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    out_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute(f"set memory_limit='{memory_limit}'")
    conn.execute("set preserve_insertion_order=false")
    conn.execute("set enable_progress_bar=false")
    stats = {"months": 0, "rows": 0, "skipped": 0, "outliers": 0}
    try:
        months = months_with_data(conn)
        newest = months[-1][0] if months else None
        for lo, hi in months:
            path = out_dir / f"month={lo:%Y-%m}.parquet"
            # the newest month is still growing; always rebuild it
            if path.exists() and not rebuild and lo != newest:
                stats["skipped"] += 1
                continue
            raw = build_month(conn, lo, hi, riders_per_line_day, seed)
            keep = pc.invert(raw.column("is_outlier"))
            stats["outliers"] += raw.num_rows - pc.sum(keep).as_py()
            table = raw.filter(keep).drop_columns(["is_outlier"])
            tmp = path.with_suffix(".tmp")
            pq.write_table(table, tmp, compression="zstd")
            fsutil.atomic_replace(tmp, path)
            stats["months"] += 1
            stats["rows"] += table.num_rows
            log.info("%s: %s rows", path.name, f"{table.num_rows:,}")
    finally:
        conn.close()
    return stats
