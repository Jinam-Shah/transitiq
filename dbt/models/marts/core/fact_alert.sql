-- One row per alert version (alert_id, last_modified_ts), entities rolled up.
-- Built from small pieces (D-026): one string_agg(DISTINCT ... ORDER BY) per version over the
-- full alert history needed > 2 GB at some DuckDB memory caps and failed in Airflow. Distinct
-- entity pairs are aggregated separately and joined; output is identical (984,017 versions).
with versions as (
    select
        alert_id,
        last_modified_ts,
        max(severity) as severity,
        min(created_ts) as created_ts,
        max(closed_ts) as closed_ts,
        min(active_start_ts) as start_ts,
        bool_or(active_start_ts is not null and active_end_ts is null) as open_ended,
        max(active_end_ts) as max_end_ts
    from {{ ref('stg_alerts') }}
    group by alert_id, last_modified_ts
),
texts as (
    select distinct on (alert_id, last_modified_ts)
        alert_id, last_modified_ts, cause, effect, effect_detail, severity_level,
        alert_lifecycle, header_text
    from {{ ref('stg_alerts') }}
),
routes as (
    select alert_id, last_modified_ts, array_to_string(list_sort(list(route_id)), ',') as routes
    from (select distinct alert_id, last_modified_ts, route_id from {{ ref('stg_alerts') }} where route_id is not null)
    group by alert_id, last_modified_ts
),
stops as (
    select alert_id, last_modified_ts, array_to_string(list_sort(list(stop_id)), ',') as stops
    from (select distinct alert_id, last_modified_ts, stop_id from {{ ref('stg_alerts') }} where stop_id is not null)
    group by alert_id, last_modified_ts
),
facilities as (
    select alert_id, last_modified_ts, count(*)::integer as facilities
    from (select distinct alert_id, last_modified_ts, facility_id from {{ ref('stg_alerts') }} where facility_id is not null)
    group by alert_id, last_modified_ts

)
select
    v.alert_id,
    v.last_modified_ts,
    t.cause,
    t.effect,
    t.effect_detail,
    v.severity,
    t.severity_level,
    t.alert_lifecycle,
    t.header_text,
    v.created_ts,
    v.closed_ts,
    v.start_ts,
    case when v.open_ended then null else v.max_end_ts end as end_ts,
    r.routes,
    s.stops,
    coalesce(f.facilities, 0) as facilities
from versions as v
join texts as t using (alert_id, last_modified_ts)
left join routes as r using (alert_id, last_modified_ts)
left join stops as s using (alert_id, last_modified_ts)
left join facilities as f using (alert_id, last_modified_ts)
