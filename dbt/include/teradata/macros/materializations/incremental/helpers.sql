{% macro teradata__get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
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


{% macro teradata__validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy") -%}
  {% if strategy == none %}
    {% set strategy = "append" %}
  {% endif %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of:  'append','delete+insert'
  {%- endset %}
  {%- if strategy not in ['append','delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%- endif %}

  {% do return(strategy) %}
{%- endmacro %}


{% macro teradata__get_incremental_sql(strategy, target_relation, tmp_relation, unique_key, dest_columns) %}
  {% if strategy == 'delete+insert' %}
    {% do return(teradata__get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'append' %}
    {% do return(teradata__get_incremental_append_sql(target_relation, tmp_relation,  dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error("Invalid Strategy") %}
  {% endif %}
{% endmacro %}