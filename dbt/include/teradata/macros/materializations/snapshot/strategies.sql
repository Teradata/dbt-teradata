
{% macro teradata__snapshot_hash_arguments(args) -%}
    HASHROW({%- for arg in args -%}
        coalesce(cast({{ arg }} AS VARCHAR(50) ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}
