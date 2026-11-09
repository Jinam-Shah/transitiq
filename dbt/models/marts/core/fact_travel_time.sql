-- One row per observed segment: previous stop -> this stop on the same trip.
select
    service_date,
    route_id,
    trunk_route_id,
    direction_id,
    trip_id,
    vehicle_id,
    prev_stop_id as from_stop_id,
    stop_id as to_stop_id,
    prev_parent_station as from_station_id,
    parent_station as to_station_id,
    move_ts as departure_ts,
    stop_ts as arrival_ts,
    {{ time_bucket('move_ts') }} as time_bucket,
    date_trunc('hour', move_ts) as weather_hour_ts,
    travel_time_seconds as travel_seconds,
    scheduled_travel_time as scheduled_seconds,
    travel_time_seconds - scheduled_travel_time as delay_seconds
from {{ ref('stg_subway_events') }}
where travel_time_seconds is not null
  and move_ts is not null
  and prev_stop_id is not null
