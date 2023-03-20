{# Overriding because the original implementation used ':' as a cast operator which is not supported in Teradata #}

{% macro teradata__current_timestamp_bk() %}
CURRENT_TIMESTAMP(6)
{% endmacro %}
