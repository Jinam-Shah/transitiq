-- 96 local-time buckets of 15 minutes. Peak periods are TransitIQ's own definition (D-015).
select
    b::integer as bucket,
    lpad(cast(b // 4 as varchar), 2, '0') || ':' || lpad(cast((b % 4) * 15 as varchar), 2, '0')
        as bucket_start,
    (b // 4)::integer as hour,
    case
        when b < 24 then 'early_morning'      -- before 06:00
        when b < 28 then 'morning'            -- 06:00-07:00
        when b < 36 then 'am_peak'            -- 07:00-09:00
        when b < 64 then 'midday'             -- 09:00-16:00
        when b < 74 then 'pm_peak'            -- 16:00-18:30
        when b < 88 then 'evening'            -- 18:30-22:00
        else 'late_night'
    end as period,
    (b between 28 and 35) or (b between 64 and 73) as is_peak
from generate_series(0, 95) as t(b)
