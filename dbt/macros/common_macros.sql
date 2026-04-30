{% macro get_current_timestamp() %}
    current_timestamp()
{% endmacro %}

{% macro get_max_date() %}
    cast('9999-12-31' as datetime)
{% endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro get_invocation_id() %}
    '{{ invocation_id }}'
{% endmacro %}
