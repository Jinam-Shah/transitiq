-- Calendar 2018-01-01 .. 2028-12-31 (covers the alerts history and the forecast horizon).
with days as (
    select cast(d as date) as date_day
    from generate_series(date '2018-01-01', date '2028-12-31', interval 1 day) as t(d)
),
holidays as (
    select holiday_date, string_agg(holiday_name, ' / ') as holiday_name
    from {{ ref('holidays') }}
    group by holiday_date
)
select
    days.date_day as date,
    extract(year from days.date_day)::integer as year,
    extract(month from days.date_day)::integer as month,
    extract(isodow from days.date_day)::integer as day_of_week,
    dayname(days.date_day) as day_name,
    extract(isodow from days.date_day) in (6, 7) as is_weekend,
    holidays.holiday_name,
    holidays.holiday_date is not null as is_holiday,
    case
        when holidays.holiday_date is not null then 'holiday'
        when extract(isodow from days.date_day) = 6 then 'saturday'
        when extract(isodow from days.date_day) = 7 then 'sunday'
        else 'weekday'
    end as day_type
from days
left join holidays on holidays.holiday_date = days.date_day
