{# UTC timestamptz -> local wall-clock timestamp (America/New_York, DST-aware). #}
{% macro local_ts(col) -%}
    ({{ col }} AT TIME ZONE '{{ var("local_timezone") }}')
{%- endmacro %}

{# 0..95: which 15-minute bucket of the local day a UTC timestamp falls in. #}
{% macro time_bucket(col) -%}
    (extract(hour from {{ local_ts(col) }}) * 4 + extract(minute from {{ local_ts(col) }}) // 15)::integer
{%- endmacro %}
