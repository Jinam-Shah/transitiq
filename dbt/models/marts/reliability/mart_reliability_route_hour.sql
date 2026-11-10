-- Reliability per route x day type x 15-minute local bucket, over all loaded history.
-- Subway travel/headway metrics come from LAMP; bus/commuter-rail ghost and bunching metrics
-- come from the real-time collector, so they exist only where it ran (D-022).
with travel as (
    select
        f.route_id, d.day_type, f.time_bucket,
        count(*) as segments,
        avg(case when f.travel_seconds <= f.scheduled_seconds + {{ var('on_time_tolerance_seconds') }}
                 then 1.0 else 0.0 end) filter (where f.scheduled_seconds > 0) as on_time_rate,
        quantile_cont(f.travel_seconds / nullif(f.scheduled_seconds, 0), 0.5) as travel_ratio_p50,
        quantile_cont(f.travel_seconds / nullif(f.scheduled_seconds, 0), 0.9) as travel_ratio_p90,
        avg(f.delay_seconds) as avg_delay_seconds
    from {{ ref('fact_travel_time') }} as f
    join {{ ref('dim_date') }} as d on d.date = f.service_date
    group by 1, 2, 3
),
headways as (
    select
        h.route_id, d.day_type, h.time_bucket,
        count(*) as arrivals,
        stddev_samp(h.headway_seconds) / nullif(avg(h.headway_seconds), 0) as headway_cv,
        avg(case when h.headway_ratio <= {{ var('reliable_headway_ratio') }} then 1.0 else 0.0 end)
            filter (where h.headway_ratio is not null) as headway_adherence
    from {{ ref('fact_headway') }} as h
    join {{ ref('dim_date') }} as d on d.date = h.service_date
    group by 1, 2, 3
),
bus_arrivals as (
    select a.route_id, d.day_type, a.time_bucket, count(*) as arrivals
    from {{ ref('fact_bus_arrival') }} as a
    join {{ ref('dim_date') }} as d on d.date = a.service_date
    where a.mode = 'bus'
    group by 1, 2, 3
),
bunches as (
    select b.route_id, d.day_type, b.time_bucket, count(*) as bunching_events
    from {{ ref('fact_bunching_event') }} as b
    join {{ ref('dim_date') }} as d on d.date = b.service_date
    group by 1, 2, 3
),
ghosts as (
    select
        g.route_id, d.day_type, g.time_bucket,
        count(*) filter (where g.status in ('ran', 'partial', 'ghost')) as judged_trips,
        count(*) filter (where g.status = 'ghost') as ghost_trips,
        count(*) filter (where g.status = 'canceled') as canceled_trips
    from {{ ref('fact_ghost_trip') }} as g
    join {{ ref('dim_date') }} as d on d.date = g.service_date
    group by 1, 2, 3
),
keys as (
    select route_id, day_type, time_bucket from travel
    union select route_id, day_type, time_bucket from headways
    union select route_id, day_type, time_bucket from bus_arrivals
    union select route_id, day_type, time_bucket from ghosts
)
select
    k.route_id,
    k.day_type,
    k.time_bucket,
    coalesce(t.segments, 0) as segments,
    t.on_time_rate,
    t.travel_ratio_p50,
    t.travel_ratio_p90,
    t.avg_delay_seconds,
    coalesce(h.arrivals, 0) + coalesce(ba.arrivals, 0) as arrivals,
    h.headway_cv,
    h.headway_adherence,
    coalesce(b.bunching_events, 0) as bunching_events,
    coalesce(b.bunching_events, 0)
        / nullif(coalesce(h.arrivals, 0) + coalesce(ba.arrivals, 0), 0)::double as bunching_index,
    coalesce(g.judged_trips, 0) as judged_trips,
    coalesce(g.ghost_trips, 0) as ghost_trips,
    coalesce(g.canceled_trips, 0) as canceled_trips,
    g.ghost_trips / nullif(g.judged_trips, 0)::double as ghost_rate
from keys as k
left join travel as t using (route_id, day_type, time_bucket)
left join headways as h using (route_id, day_type, time_bucket)
left join bus_arrivals as ba using (route_id, day_type, time_bucket)
left join bunches as b using (route_id, day_type, time_bucket)
left join ghosts as g using (route_id, day_type, time_bucket)
