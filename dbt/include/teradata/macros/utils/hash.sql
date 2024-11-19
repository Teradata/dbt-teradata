
{# The original implementation used 'md5' function which is not available in Teradata #}
{# This implementation requires that a custom UDF is installed #}
{# If variable md5_udf is not defined, the code defaults to 'GLOBAL_FUNCTIONS.hash_md5' #}
{# See README for details. #}

{% macro teradata__hash(field) -%}
    {%- set hash_md5 = var('md5_udf', 'GLOBAL_FUNCTIONS.hash_md5') -%}
    {{ hash_md5 }}(cast({{field}} AS {{type_string()}}))
{%- endmacro %}
