
{# Overriding because the original implementation used dateadd() function which is not supported in Teradata #}

{% macro teradata__dateadd(datepart, interval, from_date_or_timestamp) %}
  {% set from_date_or_timestamp_expression = from_date_or_timestamp %}
  {# we first check if 'from_date_or_timestamp' is a literal #}
  {% if from_date_or_timestamp.0 == "'" %}
    {# It is a literal #}
    {% if from_date_or_timestamp|length > '0000-00-00'|length + 2 %}
      {# we are likely dealing with a timestamp #}
      {% set from_date_or_timestamp_expression = 'cast(' ~ from_date_or_timestamp ~ ' AS TIMESTAMP(0))' %}
    {% else %}
      {% set from_date_or_timestamp_expression = 'cast(' ~ from_date_or_timestamp ~ ' AS DATE)' %}
    {% endif %}
  {% endif %}
   {{ from_date_or_timestamp_expression }}  + cast(sign({{interval}}) AS INT) * cast(abs({{ interval }}) AS INTERVAL {{datepart}}(4))
{% endmacro %}
