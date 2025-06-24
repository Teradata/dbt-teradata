{% macro teradata__dropif(node) %}

    {% set old_relation = adapter.get_relation(
        database = none,
        schema = node.schema,
        identifier = node.identifier
    ) %}

    {% set drop_relation = old_relation is not none %}

    {% set ddl = '' %}
    {% if drop_relation %}
        {% set ddl %}
          DROP FOREIGN TABLE {{ old_relation.include(database=False) }}
        {% endset %}
    {% endif %}

    {{ return(ddl) }}

{% endmacro %}