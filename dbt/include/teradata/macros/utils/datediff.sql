
{# This macro needed to be overridden because the original implementation #}
{# used 'datediff' function that is not supported on Vantage #}

{% macro teradata__datediff(first_date, second_date, datepart) %}

  {# we need to cast literals to date #}
  {% if first_date.0 == "'" %}
    {% set first_date = "cast(" + first_date + " as date)" %}
  {% endif %}

  {% if second_date.0 == "'" %}
    {% set second_date = "cast(" + second_date + " as date)" %}
  {% endif %}

  ({{ second_date }} - {{ first_date }}) {{ datepart }}(4)
{% endmacro %}
