-- Subway events with local time and the previous stop of the same trip (for segments).
select
    service_date,
    route_id,
    trunk_route_id,
    branch_route_id,
    direction_id,
    direction,
    trip_id,
    vehicle_id,
    stop_id,
    parent_station,
    stop_sequence,
    move_ts,
    stop_ts,
    travel_time_seconds,
    dwell_time_seconds,
    headway_trunk_seconds,
    headway_branch_seconds,
    scheduled_travel_time,
    scheduled_headway_trunk,
    scheduled_headway_branch,
    lag(stop_id) over trip_order as prev_stop_id,
    lag(parent_station) over trip_order as prev_parent_station
from {{ source('silver', 'subway_events') }}
window trip_order as (partition by service_date, trip_id, vehicle_id order by stop_sequence)
