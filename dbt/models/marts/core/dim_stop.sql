select
    stop_id,
    stop_name,
    parent_station,
    coalesce(parent_station, stop_id) as station_id,
    platform_name,
    stop_lat as lat,
    stop_lon as lon,
    location_type,
    wheelchair_boarding,
    municipality,
    vehicle_type
from {{ ref('stg_gtfs_stops') }}
