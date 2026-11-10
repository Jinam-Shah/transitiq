-- Windows of continuous real-time collection: a gap longer than rt_max_gap_minutes between
-- vehicle-position snapshots starts a new window. Only trips inside a window are judged.
with snapshots as (
    select distinct feed_ts from {{ ref('stg_rt_vehicle_positions') }}
),
flagged as (
    select
        feed_ts,
        case
            when lag(feed_ts) over (order by feed_ts) is null
              or feed_ts - lag(feed_ts) over (order by feed_ts) > to_minutes({{ var('rt_max_gap_minutes') }})
            then 1 else 0
        end as new_window
    from snapshots
),
numbered as (
    select feed_ts, sum(new_window) over (order by feed_ts) as window_id from flagged
)
select window_id, min(feed_ts) as window_start, max(feed_ts) as window_end, count(*) as snapshots
from numbered
group by window_id
