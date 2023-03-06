{# The default implementation returns 'string' which is not a supported type in Teradata #}
{# Here, we say 'LONG VARCHAR' as we do not know how the type will be used. In most scenarios #}
{# it is better to use VARCHAR of specific size rather than calling this macro #}

{%- macro teradata__type_string() -%}
    LONG VARCHAR
{%- endmacro -%}
