
{% macro teradata__snapshot_merge_sql_update(target, source, insert_cols) -%}
    UPDATE {{ target }}
    FROM (SELECT dbt_scd_id, dbt_change_type, dbt_valid_to FROM {{ source }}) AS DBT_INTERNAL_SOURCE
    SET dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    WHERE DBT_INTERNAL_SOURCE.dbt_scd_id = {{ target }}.dbt_scd_id
      AND DBT_INTERNAL_SOURCE.dbt_change_type = 'update'
      AND {{ target }}.dbt_valid_to IS NULL
{% endmacro %}

{% macro teradata__snapshot_merge_sql_insert(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    INSERT INTO {{ target }} ({{ insert_cols_csv }})
    SELECT {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    FROM {{ source }} AS DBT_INTERNAL_SOURCE
    WHERE DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
{% endmacro %}

{% macro teradata__snapshot_merge_sql_delete(target, source, insert_cols) -%}
    UPDATE {{ target }}
    FROM (SELECT dbt_scd_id, dbt_change_type, dbt_valid_to FROM {{ source }}) AS DBT_INTERNAL_SOURCE
    SET dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    WHERE DBT_INTERNAL_SOURCE.dbt_scd_id = {{ target }}.dbt_scd_id
      AND DBT_INTERNAL_SOURCE.dbt_change_type = 'delete'
      AND {{ target }}.dbt_valid_to IS NULL
{% endmacro %}
