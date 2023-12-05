-- This macro is overriden because limit keyword doesnot work in teradata database and has been replaced with top keyword
{% macro teradata__get_limit_subquery_sql(sql, limit) %}
    select top {{ limit }} *
    from (
        {{ sql }}
    ) as model_limit_subq
{% endmacro %}