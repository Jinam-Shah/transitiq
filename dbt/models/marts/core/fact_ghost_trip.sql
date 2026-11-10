-- One row per scheduled commuter-rail / bus / ferry trip that the real-time collector fully
-- covered. status: ran | partial (vanished partway) | ghost (never seen with a vehicle) |
-- canceled (announced as CANCELED in TripUpdates). D-022.
with judged as (
    select s.*
    from {{ ref('int_scheduled_trips') }} as s
    join {{ ref('int_rt_coverage') }} as c
      on s.scheduled_start_ts - to_minutes({{ var('ghost_grace_minutes') }}) >= c.window_start
     and s.scheduled_end_ts + to_minutes({{ var('ghost_grace_minutes') }}) <= c.window_end
),
seen as (
    select
        trip_id,
        strptime(start_date, '%Y%m%d')::date as service_date,
        count(distinct feed_ts)::integer as snapshots_seen,
        min(feed_ts) as first_seen_ts,
        max(feed_ts) as last_seen_ts,
        max(current_stop_sequence) as max_stop_sequence_seen
    from {{ ref('stg_rt_vehicle_positions') }}
    where schedule_relationship = 'SCHEDULED' and trip_id is not null and start_date is not null
    group by 1, 2
),
predicted as (
    select
        trip_id,
        strptime(start_date, '%Y%m%d')::date as service_date,
        bool_or(schedule_relationship = 'CANCELED') as canceled
    from {{ ref('stg_rt_trip_updates') }}
    where start_date is not null
    group by 1, 2
)
select
    j.service_date,
    j.trip_id,
    j.route_id,
    j.route_type,
    j.direction_id,
    j.scheduled_start_ts,
    j.scheduled_end_ts,
    {{ time_bucket('j.scheduled_start_ts') }} as time_bucket,
    case
        when coalesce(p.canceled, false) then 'canceled'
        when s.trip_id is null then 'ghost'
        when s.max_stop_sequence_seen >= {{ var('ran_min_stop_share') }} * j.max_stop_sequence
          or s.last_seen_ts >= j.scheduled_end_ts - to_minutes({{ var('ran_end_slack_minutes') }})
            then 'ran'
        else 'partial'
    end as status,
    p.trip_id is not null as was_predicted,
    coalesce(s.snapshots_seen, 0) as snapshots_seen,
    s.first_seen_ts,
    s.last_seen_ts,
    s.max_stop_sequence_seen,
    j.max_stop_sequence as max_stop_sequence_scheduled
from judged as j
left join seen as s on s.trip_id = j.trip_id and s.service_date = j.service_date
left join predicted as p on p.trip_id = j.trip_id and p.service_date = j.service_date
