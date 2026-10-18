{# A quarantine table only exists when a dataset had bad rows. #}
{# This macro joins (union all) the quarantine tables that exist, and skips the ones that do not. #}
{% macro quarantine_union() %}

{% set selects = [] %}

{% if execute %}
    {% for dataset in var('datasets') %}
        {% set table = adapter.get_relation(database=target.database, schema='quarantine', identifier=dataset) %}
        {% if table is not none %}
            {% do selects.append(
                "select '" ~ dataset ~ "' as dataset, _quarantine_status as quarantine_status, "
                ~ "_quarantined_at as quarantined_at, _failure_reasons as failure_reasons from " ~ table
            ) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if selects | length == 0 %}
    {# no quarantine tables yet: return an empty result with the right columns #}
    select cast(null as string) as dataset, cast(null as string) as quarantine_status,
           cast(null as timestamp) as quarantined_at, cast(null as string) as failure_reasons
    where 1 = 0
{% else %}
    {{ selects | join(' union all ') }}
{% endif %}

{% endmacro %}
