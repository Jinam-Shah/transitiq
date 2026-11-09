{# Use the configured schema name as-is (staging, gold, reference) instead of dbt's
   default "<target>_<custom>" prefixing, so tables are gold.dim_route, not main_gold.dim_route. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
