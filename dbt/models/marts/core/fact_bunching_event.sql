-- Bunching: a vehicle arriving within 25% of the scheduled headway of the vehicle before it at
-- the same stop. Subway events come from LAMP history (headways observed by MBTA); bus events
-- come from our GTFS-Realtime collector (fact_bus_arrival). gap_before_seconds is the headway
-- just before the bunch; after_long_gap marks the classic "long wait, then two at once".
-- Bus headways never span a gap in collection (D-024): the first bus seen after a gap has no
-- headway, so a collector outage cannot look like a long wait.
with subway as (
    select
        *,
        lag(headway_seconds) over (partition by service_date, stop_id order by arrival_ts)
            as gap_before_seconds
    from {{ ref('fact_headway') }}
),
bus_arrivals as (
    select
        *,
        date_diff('second',
                  lag(arrival_ts) over (partition by service_date, route_id, direction_id, stop_id, coverage_window_start
                                        order by arrival_ts),
                  arrival_ts) as headway_seconds
    from {{ ref('fact_bus_arrival') }}
    where mode = 'bus'
),
bus as (
    select
        *,
        lag(headway_seconds) over (partition by service_date, route_id, direction_id, stop_id, coverage_window_start
                                   order by arrival_ts) as gap_before_seconds
    from bus_arrivals
)
select
    'subway' as mode,
    service_date, route_id, trunk_route_id, direction_id, stop_id, station_id,
    arrival_ts as ts, time_bucket, 2 as vehicles_involved,
    headway_seconds, scheduled_headway_seconds, gap_before_seconds,
    coalesce(gap_before_seconds >= {{ var('long_gap_headway_ratio') }} * scheduled_headway_seconds, false)
        as after_long_gap
from subway
where scheduled_headway_seconds is not null
  and headway_seconds <= {{ var('bunching_headway_ratio') }} * scheduled_headway_seconds
union all
select
    'bus' as mode,
    b.service_date, b.route_id, b.route_id as trunk_route_id, b.direction_id, b.stop_id,
    coalesce(s.parent_station, b.stop_id) as station_id,
    b.arrival_ts as ts, b.time_bucket, 2 as vehicles_involved,
    b.headway_seconds, b.scheduled_headway_seconds::bigint, b.gap_before_seconds,
    coalesce(b.gap_before_seconds >= {{ var('long_gap_headway_ratio') }} * b.scheduled_headway_seconds, false)
        as after_long_gap
from bus as b
left join {{ ref('stg_gtfs_stops') }} as s on s.stop_id = b.stop_id
where b.scheduled_headway_seconds > 0
  and b.headway_seconds is not null
  and b.headway_seconds <= {{ var('bunching_headway_ratio') }} * b.scheduled_headway_seconds
