{#
  dbt's DEFAULT generate_schema_name behavior prefixes any custom +schema
  config with the connection's default schema (profiles.yml's `schema:
  bronze`), e.g. +schema: silver becomes "bronze_silver" instead of plain
  "silver". This overrides that so models land exactly where docker/init.sql
  created the medallion schemas (bronze/silver/gold), matching the project's
  documented architecture.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}