select * exclude (_source_file, _source_sha256, _loaded_at)
from {{ source('silver', 'gtfs_routes') }}
