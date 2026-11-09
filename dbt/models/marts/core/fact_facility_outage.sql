-- One row per elevator/escalator outage (alert) and facility. end_ts is null while ongoing.
with closures as (
    select *
    from {{ ref('stg_alerts') }}
    where effect_detail in ('ELEVATOR_CLOSURE', 'ESCALATOR_CLOSURE')
      and facility_id is not null
),
outages as (
    select
        alert_id,
        facility_id,
        any_value(effect_detail) as effect_detail,
        min(stop_id) as stop_id,
        min(coalesce(active_start_ts, created_ts)) as start_ts,
        coalesce(max(closed_ts), max(active_end_ts)) as end_ts
    from closures
    group by alert_id, facility_id
)
select
    outages.alert_id,
    outages.facility_id,
    case outages.effect_detail
        when 'ELEVATOR_CLOSURE' then 'elevator'
        else 'escalator'
    end as facility_type,
    facilities.long_name as facility_name,
    outages.stop_id,
    coalesce(stops.parent_station, outages.stop_id) as station_id,
    outages.start_ts,
    case when outages.end_ts >= outages.start_ts then outages.end_ts end as end_ts,
    case
        when outages.end_ts >= outages.start_ts
            then date_diff('minute', outages.start_ts, outages.end_ts) / 60.0
    end as duration_hours
from outages
left join {{ ref('stg_facilities') }} as facilities on facilities.facility_id = outages.facility_id
left join {{ ref('stg_gtfs_stops') }} as stops on stops.stop_id = outages.stop_id
