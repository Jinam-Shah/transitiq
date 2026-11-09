-- One row per observed arrival at a stop with the gap since the previous train (trunk level).
select
    service_date,
    route_id,
    trunk_route_id,
    branch_route_id,
    direction_id,
    stop_id,
    parent_station as station_id,
    stop_ts as arrival_ts,
    {{ time_bucket('stop_ts') }} as time_bucket,
    headway_trunk_seconds as headway_seconds,
    nullif(scheduled_headway_trunk, 0) as scheduled_headway_seconds,
    headway_branch_seconds,
    nullif(scheduled_headway_branch, 0) as scheduled_headway_branch_seconds,
    headway_trunk_seconds / nullif(scheduled_headway_trunk, 0) as headway_ratio
from {{ ref('stg_subway_events') }}
where headway_trunk_seconds is not null
  and stop_ts is not null
