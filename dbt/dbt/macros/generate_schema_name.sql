{# By default dbt writes "gold_gold_staging" (target schema + our schema). #}
{# This tiny macro makes dbt use exactly the schema name from dbt_project.yml (gold, gold_staging, gold_tests). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
