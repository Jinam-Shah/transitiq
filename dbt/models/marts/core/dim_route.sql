select
    route_id,
    coalesce(nullif(route_long_name, ''), route_short_name) as route_name,
    route_short_name,
    route_type,
    case route_type
        when 0 then 'light_rail'
        when 1 then 'subway'
        when 2 then 'commuter_rail'
        when 3 then 'bus'
        when 4 then 'ferry'
        else 'other'
    end as mode,
    case when route_color is not null then '#' || route_color end as color,
    line_id,
    route_sort_order
from {{ ref('stg_gtfs_routes') }}
