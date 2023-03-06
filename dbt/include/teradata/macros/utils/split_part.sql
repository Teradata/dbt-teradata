{# The function used in the original macro ('split_part') is called 'strtok' in Teradata #}

{% macro default__split_part(string_text, delimiter_text, part_number) %}
  {# if delimiter_text is a single character literal #}
  {% if delimiter_text|length == 3 and delimiter_text.0  =="'" and delimiter_text.2  == "'" %}
    strtok(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
        )
  {% else %}
    -- This is a hack. We are replacing the delimiter text with chr(1). We then let strtok() tokenize.
    -- chr(1) Is a unique character. If there is chr(1) in the search string this query will produce hard-to-debug errors.
    strtok(oreplace({{ string_text }}, {{ delimiter_text }}, chr(1)),
        chr(1),
        {{ part_number }}
        )
  {% endif %}
{% endmacro %}
