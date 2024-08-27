-- This macro is overriden because limit keyword doesnot work in teradata database and has been replaced with sample keyword
{% macro teradata__get_limit_sql(sql, limit) %}
   {{ compiled_code }}
  {% if limit is not none %}
  sample {{ limit }}
  {%- endif -%}
{% endmacro %}