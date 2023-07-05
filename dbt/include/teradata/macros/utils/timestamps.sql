{# Overriding because the original implementation used ':' as a cast operator which is not supported in Teradata #}

{% macro teradata__current_timestamp() %}
CURRENT_TIMESTAMP(6)
{% endmacro %}

{% macro teradata__snapshot_string_as_time(timestamp) -%}
    {%- set timestamp_string = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f%z') -%}
    {%- set result = "TO_TIMESTAMP_TZ('" ~ "{0}:{1}".format(timestamp_string[:-2], timestamp_string[-2:]) ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}
