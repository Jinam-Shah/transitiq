-- Every scheduled stop of the trips in int_scheduled_trips, with the scheduled headway to the
-- previous scheduled trip at the same stop, route and direction (used for bus bunching).
select
    st.service_date,
    st.trip_id,
    st.route_id,
    st.route_type,
    st.direction_id,
    s.stop_id,
    s.stop_sequence,
    timezone('{{ var("local_timezone") }}',
             st.service_date::timestamp + to_seconds(coalesce(s.arrival_seconds, s.departure_seconds)))
        as scheduled_arrival_ts,
    date_diff(
        'second',
        lag(timezone('{{ var("local_timezone") }}',
                     st.service_date::timestamp + to_seconds(coalesce(s.arrival_seconds, s.departure_seconds))))
            over w,
        timezone('{{ var("local_timezone") }}',
                 st.service_date::timestamp + to_seconds(coalesce(s.arrival_seconds, s.departure_seconds)))
    ) as scheduled_headway_seconds
from {{ ref('int_scheduled_trips') }} as st
join {{ ref('stg_gtfs_stop_times') }} as s on s.trip_id = st.trip_id
window w as (
    partition by st.service_date, st.route_id, st.direction_id, s.stop_id
    order by coalesce(s.arrival_seconds, s.departure_seconds)
)
