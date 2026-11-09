-- Historical subway bunching from observed headways: a train arriving within 25% of the
-- scheduled headway of the train before it. gap_before_seconds is the headway just before.
with ordered as (
    select
        *,
        lag(headway_seconds) over (
            partition by service_date, stop_id order by arrival_ts
        ) as gap_before_seconds
    from {{ ref('fact_headway') }}
)
select
    service_date,
    route_id,
    trunk_route_id,
    direction_id,
    stop_id,
    station_id,
    arrival_ts as ts,
    time_bucket,
    2 as vehicles_involved,
    headway_seconds,
    scheduled_headway_seconds,
    gap_before_seconds,
    coalesce(gap_before_seconds >= {{ var('long_gap_headway_ratio') }} * scheduled_headway_seconds, false)
        as after_long_gap
from ordered
where scheduled_headway_seconds is not null
  and headway_seconds <= {{ var('bunching_headway_ratio') }} * scheduled_headway_seconds
