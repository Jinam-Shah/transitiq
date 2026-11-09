-- Wait-time reliability per stop (platform) x day type x 15-minute local bucket.
-- expected_wait_seconds: average wait for a rider arriving at a random time = E[h^2] / (2 E[h]).
select
    h.stop_id,
    any_value(h.station_id) as station_id,
    d.day_type,
    h.time_bucket,
    count(*) as arrivals,
    sum(h.headway_seconds * h.headway_seconds) / (2.0 * sum(h.headway_seconds)) as expected_wait_seconds,
    quantile_cont(h.headway_seconds, 0.5) as headway_p50_seconds,
    quantile_cont(h.headway_seconds, 0.9) as headway_p90_seconds,
    quantile_cont(h.headway_seconds, 0.5) / 2.0 as wait_p50_seconds,
    quantile_cont(h.headway_seconds, 0.9) / 2.0 as wait_p90_seconds,
    avg(case when h.headway_ratio <= {{ var('reliable_headway_ratio') }} then 1.0 else 0.0 end)
        filter (where h.headway_ratio is not null) as reliability_score
from {{ ref('fact_headway') }} as h
join {{ ref('dim_date') }} as d on d.date = h.service_date
group by h.stop_id, d.day_type, h.time_bucket
