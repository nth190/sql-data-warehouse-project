{% macro generate_surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

{% macro get_scd2_columns() %}
    valid_from,
    valid_to,
    is_current
{% endmacro %}
