
{% macro teradata__date_trunc(datepart, date) %}
    trunc(cast({{date}} AS DATE), '{{datepart}}')
{% endmacro %}


