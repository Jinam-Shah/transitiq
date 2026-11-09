-- One row per alert version (alert_id, last_modified_ts), entities rolled up.
select
    alert_id,
    last_modified_ts,
    any_value(cause) as cause,
    any_value(effect) as effect,
    any_value(effect_detail) as effect_detail,
    max(severity) as severity,
    any_value(severity_level) as severity_level,
    any_value(alert_lifecycle) as alert_lifecycle,
    any_value(header_text) as header_text,
    min(created_ts) as created_ts,
    max(closed_ts) as closed_ts,
    min(active_start_ts) as start_ts,
    case
        when count(*) filter (where active_start_ts is not null and active_end_ts is null) > 0
            then null
        else max(active_end_ts)
    end as end_ts,
    string_agg(distinct route_id, ',' order by route_id) as routes,
    string_agg(distinct stop_id, ',' order by stop_id) as stops,
    count(distinct facility_id)::integer as facilities
from {{ ref('stg_alerts') }}
group by alert_id, last_modified_ts
