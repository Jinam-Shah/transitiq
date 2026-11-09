-- One row per 15 minutes of the day.
select count(*) as n from {{ ref('dim_time_bucket') }} having count(*) <> 96
