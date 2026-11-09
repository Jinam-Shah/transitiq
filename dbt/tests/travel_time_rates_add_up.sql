-- Every mart row must be backed by at least one observation.
select * from {{ ref('mart_reliability_route_hour') }}
where coalesce(segments, 0) = 0 and coalesce(arrivals, 0) = 0
