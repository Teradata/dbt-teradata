
{% macro build_source_image(source_table, incremental_strategy, valid_from, valid_period, exclude_columns, config) %}

{%- set incremental_strategy = incremental_strategy if incremental_strategy else (config.get('incremental_strategy', None) if config) -%}
{%- set valid_from = valid_from if valid_from else (config.get('valid_from', None) if config) -%}
{%- set valid_period = valid_period if valid_period else (config.get('valid_period', None) if config) -%}
{%- set exclude_columns = exclude_columns if exclude_columns else (config.get('exclude_columns', []) if config else []) -%}

{%- if incremental_strategy=='valid_history' %}
    {%- set valid_to_value = "'9999-12-31 23:59:59.999999+00:00' (timestamp)" -%}
    {%- if valid_from and valid_from is not none -%}
        {%- set valid_from_value = valid_from -%}
        {%- set exclude_columns = exclude_columns + [valid_from] -%} 
    {%- else -%}
        {%- set valid_from_value = "current_timestamp" -%}
    {%- endif %}
{%- endif %}

{%- set firstcol = [] %}

select
{%- for column in adapter.get_columns_in_relation(ref(source_table)) -%}
    {%- if column.name not in exclude_columns %}
        {%- if firstcol == [] %}
            {%- do firstcol.append(True) -%}
        {%- else -%}
            ,
        {%- endif %}    
    {{ column.name }}
    {%- elif column.name == valid_from -%}
        {% if column.is_date %}
        {% set valid_to_value = "'9999-12-31' (date)" %}
        {% endif %}
    {% endif %}    
{%- endfor -%}
{%- if incremental_strategy=='valid_history' %}
,period({{ valid_from_value }}, {{ valid_to_value }}) as {{ valid_period }}
{%- endif %}
from {{ ref(source_table) }} source
 
 {% endmacro %}