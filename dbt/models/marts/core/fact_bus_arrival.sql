-- Estimated arrival of a real-time trip at a stop: the last prediction made while that stop was
-- still the trip's NEXT stop, kept only once the trip moved on to a later stop (so the stop was
-- really reached), only if the prediction was fresh, and only for trips actually seen with a
-- vehicle: a ghost trip's predictions keep advancing, and counting them would invent arrivals
-- (and fake bunches) for a bus that never ran. D-022.
-- coverage_window_start: the collector window the prediction came from; headways are only
-- measured within one window, never across a collection gap (D-024).
with ran as (
    select distinct strptime(start_date, '%Y%m%d')::date as service_date, trip_id
    from {{ ref('stg_rt_vehicle_positions') }}
    where schedule_relationship = 'SCHEDULED' and trip_id is not null and start_date is not null
),
nexts as (
    select
        strptime(start_date, '%Y%m%d')::date as service_date,
        trip_id, route_id, direction_id, vehicle_id,
        next_stop_sequence as stop_sequence,
        next_stop_id as stop_id,
        coalesce(next_arrival_ts, next_departure_ts) as predicted_ts,
        feed_ts,
        row_number() over (
            partition by start_date, trip_id, next_stop_sequence order by feed_ts desc
        ) as rn,
        max(next_stop_sequence) over (partition by start_date, trip_id) as furthest_sequence
    from {{ ref('stg_rt_trip_updates') }}
    where schedule_relationship = 'SCHEDULED'
      and start_date is not null and next_stop_sequence is not null
),
arrivals as (
select
    n.service_date,
    n.trip_id,
    n.route_id,
    r.route_type,
    case when r.route_type = 3 then 'bus' when r.route_type = 2 then 'commuter_rail'
         when r.route_type = 4 then 'ferry' else 'rail' end as mode,
    n.direction_id,
    n.vehicle_id,
    n.stop_id,
    n.stop_sequence,
    n.predicted_ts as arrival_ts,
    c.window_start as coverage_window_start,
    {{ time_bucket('n.predicted_ts') }} as time_bucket,
    date_diff('second', n.feed_ts, n.predicted_ts) as prediction_lead_seconds,
    sst.scheduled_arrival_ts,
    sst.scheduled_headway_seconds,
    date_diff('second', sst.scheduled_arrival_ts, n.predicted_ts) as delay_seconds
from nexts as n
join ran on ran.service_date = n.service_date and ran.trip_id = n.trip_id
join {{ ref('stg_gtfs_routes') }} as r on r.route_id = n.route_id
-- windows come from VehiclePositions timestamps; a TripUpdates snapshot can be a few seconds
-- outside them, so allow 1 minute (windows are always more than 5 minutes apart)
join {{ ref('int_rt_coverage') }} as c
  on n.feed_ts between c.window_start - to_minutes(1) and c.window_end + to_minutes(1)
left join {{ ref('int_scheduled_stop_times') }} as sst
  on sst.service_date = n.service_date and sst.trip_id = n.trip_id
 and sst.stop_sequence = n.stop_sequence
where n.rn = 1
  and n.stop_sequence < n.furthest_sequence
  and n.predicted_ts is not null
  and abs(date_diff('second', n.feed_ts, n.predicted_ts)) <= {{ var('max_prediction_age_minutes') }} * 60
)
-- The same vehicle reported at the same stop again within 2 minutes (e.g. under two trip ids
-- around a terminal) is one arrival, not two buses: keep the first (D-026).
select * exclude (prev_same_vehicle_ts)
from (
    select
        *,
        lag(arrival_ts) over (
            partition by service_date, vehicle_id, stop_id order by arrival_ts, trip_id
        ) as prev_same_vehicle_ts
    from arrivals
)
where vehicle_id is null
   or prev_same_vehicle_ts is null
   or arrival_ts - prev_same_vehicle_ts > interval 2 minute
