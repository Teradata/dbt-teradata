
{% macro teradata__snapshot_merge_sql_update(target, source, insert_cols) -%}
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}
    UPDATE {{ target }}
    FROM (SELECT {{ columns.dbt_scd_id }}, dbt_change_type, {{ columns.dbt_valid_to }} FROM {{ source }}) AS DBT_INTERNAL_SOURCE
    SET {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}
    WHERE DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }} = {{ target }}.{{ columns.dbt_scd_id }}
      AND DBT_INTERNAL_SOURCE.dbt_change_type = 'update'
      {% if config.get("dbt_valid_to_current") %}
       AND ({{ target }}.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }} or
            {{ target }}.{{ columns.dbt_valid_to }} is null)
     {% else %}
       AND {{ target }}.{{ columns.dbt_valid_to }} is null
     {% endif %}
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
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}
    UPDATE {{ target }}
    FROM (SELECT {{ columns.dbt_scd_id }}, dbt_change_type, {{ columns.dbt_valid_to }} FROM {{ source }}) AS DBT_INTERNAL_SOURCE
    SET {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}
    WHERE DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }} = {{ target }}.{{ columns.dbt_scd_id }}
      AND DBT_INTERNAL_SOURCE.dbt_change_type = 'delete'
      {% if config.get("dbt_valid_to_current") %}
       AND ({{ target }}.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }} or
            {{ target }}.{{ columns.dbt_valid_to }} is null)
     {% else %}
       AND {{ target }}.{{ columns.dbt_valid_to }} is null
     {% endif %}
      
{% endmacro %}
