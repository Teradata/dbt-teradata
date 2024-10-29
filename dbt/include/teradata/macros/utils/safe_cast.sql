{% macro teradata__safe_cast(field, type) %}
    {%- set field_as_string =  dbt.string_literal(field) if field is number else field -%}
    trycast({{field_as_string}} as {{type}})
{% endmacro %}