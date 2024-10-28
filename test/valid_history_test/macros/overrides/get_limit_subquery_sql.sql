{% macro get_limit_subquery_sql(sql, limit) %}
    {# Check if a limit is defined and the query does not already use a sampling method #}
    {%- if limit is not none and use_limit(sql) -%}
        {# Check if we must use a subquery (eg. set operators) in which case we use a TOP statement #}
        {%- if use_limit_subquery(sql) is sameas true -%}
            select top {{limit}} * from ({{sql}}) as query
        {%- else -%}
            {{sql}} sample {{limit}}
        {%- endif -%}
    {%- else -%}
        {{ sql }}
    {%- endif -%}
{% endmacro %}

{# Check if the query already uses a TOP or SAMPLE keyword #}
{% macro use_limit(sql) %}
    {% set sql_words = sql.lower().split() %}
    {%- do return(not('top' in sql_words or 'sample' in sql_words)) -%}
{% endmacro %}

{# Check if the query uses set operators #}
{% macro use_limit_subquery(sql) %}
    {% set sql_words = sql.lower().split() %}
    {%- do return('union' in sql_words or 'minus' in sql_words or 'intersects' in sql_words) -%}
{% endmacro %}