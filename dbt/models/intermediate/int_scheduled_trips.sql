-- Scheduled trips (commuter rail, bus, ferry) for every service date the collector touched.
-- GTFS times are seconds after local midnight of the service date (may exceed 24h).
with dates as (
    select distinct cast(d as date) as service_date
    from {{ ref('int_rt_coverage') }},
        generate_series(
            cast({{ local_ts('window_start') }} as date) - 1,
            cast({{ local_ts('window_end') }} as date),
            interval 1 day
        ) as t(d)
),
by_weekday as (
    select d.service_date, c.service_id
    from dates as d
    join {{ ref('stg_gtfs_calendar') }} as c
      on d.service_date between c.start_date and c.end_date
     and 1 = case isodow(d.service_date)
                 when 1 then c.monday when 2 then c.tuesday when 3 then c.wednesday
                 when 4 then c.thursday when 5 then c.friday when 6 then c.saturday
                 else c.sunday end
),
active as (
    select service_date, service_id from by_weekday
    union
    select cd.date, cd.service_id
    from {{ ref('stg_gtfs_calendar_dates') }} as cd join dates as d on d.service_date = cd.date
    where cd.exception_type = 1
    except
    select cd.date, cd.service_id
    from {{ ref('stg_gtfs_calendar_dates') }} as cd join dates as d on d.service_date = cd.date
    where cd.exception_type = 2
),
trip_times as (
    select
        trip_id,
        min(coalesce(departure_seconds, arrival_seconds)) as start_seconds,
        max(coalesce(arrival_seconds, departure_seconds)) as end_seconds,
        max(stop_sequence) as max_stop_sequence,
        count(*) as stops
    from {{ ref('stg_gtfs_stop_times') }}
    group by trip_id
)
select
    a.service_date,
    t.trip_id,
    t.route_id,
    r.route_type,
    t.direction_id,
    timezone('{{ var("local_timezone") }}', a.service_date::timestamp + to_seconds(tt.start_seconds))
        as scheduled_start_ts,
    timezone('{{ var("local_timezone") }}', a.service_date::timestamp + to_seconds(tt.end_seconds))
        as scheduled_end_ts,
    tt.max_stop_sequence,
    tt.stops
from active as a
join {{ ref('stg_gtfs_trips') }} as t on t.service_id = a.service_id
join {{ ref('stg_gtfs_routes') }} as r on r.route_id = t.route_id
join trip_times as tt on tt.trip_id = t.trip_id
where r.route_type in ({{ var('rt_route_types') | join(', ') }})
