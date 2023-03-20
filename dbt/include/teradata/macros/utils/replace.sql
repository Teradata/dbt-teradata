{# The original implementation used 'replace' function. In Teradata, the function is called 'oreplace' #}

{% macro teradata__replace(field, old_chars, new_chars) %}

    oreplace(
        {{ field }},
        {{ old_chars }},
        {{ new_chars }}
    )

{% endmacro %}