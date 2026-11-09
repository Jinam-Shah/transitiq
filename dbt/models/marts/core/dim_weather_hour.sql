-- One row per hour; condition groups WMO weather codes.
select
    hour_ts,
    cast({{ local_ts('hour_ts') }} as date) as local_date,
    extract(hour from {{ local_ts('hour_ts') }})::integer as local_hour,
    temperature_2m_c as temperature_c,
    relative_humidity_2m_pct as humidity_pct,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    snow_depth_m,
    wind_speed_10m_kmh as wind_speed_kmh,
    wind_gusts_10m_kmh as wind_gusts_kmh,
    weather_code,
    case
        when weather_code = 0 then 'clear'
        when weather_code between 1 and 3 then 'cloudy'
        when weather_code in (45, 48) then 'fog'
        when weather_code between 51 and 57 then 'drizzle'
        when weather_code between 61 and 67 or weather_code between 80 and 82 then 'rain'
        when weather_code between 71 and 77 or weather_code in (85, 86) then 'snow'
        when weather_code between 95 and 99 then 'thunderstorm'
        else 'unknown'
    end as condition,
    coalesce(snowfall_cm, 0) > 0 as is_snow,
    coalesce(rain_mm, 0) > 0 as is_rain
from {{ ref('stg_weather_hourly') }}
