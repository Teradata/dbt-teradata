{% macro incremental_upsert(on_schema_change, tmp_relation, target_relation, existing_relation, unique_key=none, statement_name="main") %}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
        {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {% endif %}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {%- if unique_key is not none -%}
    DELETE
    FROM {{ target_relation }}
    WHERE ({{ unique_key }}) IN (
        SELECT ({{ unique_key }})
        FROM {{ tmp_relation }}
    );
    {%- endif %}

    INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
       SELECT {{ dest_cols_csv }}
       FROM {{ tmp_relation }}
    ;
{%- endmacro %}
