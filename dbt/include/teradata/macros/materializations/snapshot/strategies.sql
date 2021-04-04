
{% macro teradata__snapshot_hash_arguments(args) -%}
    HASHROW({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar(50) ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}
