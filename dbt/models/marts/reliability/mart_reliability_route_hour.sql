-- Reliability per route x day type x 15-minute local bucket, over all loaded history.
with travel as (
    select
        f.route_id,
        d.day_type,
        f.time_bucket,
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
        h.route_id,
        d.day_type,
        h.time_bucket,
        count(*) as arrivals,
        stddev_samp(h.headway_seconds) / nullif(avg(h.headway_seconds), 0) as headway_cv,
        avg(case when h.headway_ratio <= {{ var('reliable_headway_ratio') }} then 1.0 else 0.0 end)
            filter (where h.headway_ratio is not null) as headway_adherence
    from {{ ref('fact_headway') }} as h
    join {{ ref('dim_date') }} as d on d.date = h.service_date
    group by 1, 2, 3
),
bunches as (
    select b.route_id, d.day_type, b.time_bucket, count(*) as bunching_events
    from {{ ref('fact_bunching_event') }} as b
    join {{ ref('dim_date') }} as d on d.date = b.service_date
    group by 1, 2, 3
)
select
    coalesce(t.route_id, h.route_id) as route_id,
    coalesce(t.day_type, h.day_type) as day_type,
    coalesce(t.time_bucket, h.time_bucket) as time_bucket,
    coalesce(t.segments, 0) as segments,
    t.on_time_rate,
    t.travel_ratio_p50,
    t.travel_ratio_p90,
    t.avg_delay_seconds,
    coalesce(h.arrivals, 0) as arrivals,
    h.headway_cv,
    h.headway_adherence,
    coalesce(b.bunching_events, 0) as bunching_events,
    coalesce(b.bunching_events, 0) / nullif(h.arrivals, 0)::double as bunching_index,
    cast(null as double) as ghost_rate  -- filled from GTFS-Realtime sampling in Phase 6
from travel as t
full outer join headways as h
    on h.route_id = t.route_id and h.day_type = t.day_type and h.time_bucket = t.time_bucket
left join bunches as b
    on b.route_id = coalesce(t.route_id, h.route_id)
   and b.day_type = coalesce(t.day_type, h.day_type)
   and b.time_bucket = coalesce(t.time_bucket, h.time_bucket)
