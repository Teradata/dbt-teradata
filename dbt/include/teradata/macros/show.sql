{% macro teradata__get_limit_subquery_sql(sql, limit) %}
    select top {{ limit }} *
    from (
        {{ sql }}
    ) as model_limit_subq
{% endmacro %}