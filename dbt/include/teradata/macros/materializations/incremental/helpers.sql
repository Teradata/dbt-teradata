{% macro teradata__validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy") -%}
  {% if strategy == none %}
    {% set strategy = "append" %}
  {% endif %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of:  'append','delete+insert','merge', 'valid_history'
  {%- endset %}
  {%- if strategy not in ['append','delete+insert','merge', 'valid_history'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%- endif %}
  {% do return(strategy) %}
{%- endmacro %}

{% macro get_insert_sql(target_relation, ins_col_list, sql) %}
{#-
-- Description: Genereate full insert-select SQL statement
-- Parameters:
--     target (relation): Schema and name of target table
--     ins_col_list: List of insert columns of target table, it should match the model's select sql
--     sql (string): The select SQL as defined in the current model
-#}
INSERT INTO {{ target_relation }} (
  {% for col_name in ins_col_list %}
    {% if loop.first %} {% else %},{% endif %}{{ adapter.quote(col_name) }}
  {%- endfor %}
    )
    {{ sql }}

{% endmacro %}

{% macro teradata__get_incremental_sql(strategy, target_relation, tmp_relation, unique_key, dest_columns,incremental_predicates,
valid_period, valid_from, valid_to, use_valid_to_time, history_column_in_target, resolve_conflicts) %}
  {% if strategy == 'delete+insert' %}
    {% do return(teradata__get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates)) %}
  {% elif strategy == 'append' %}
    {% do return(teradata__get_incremental_append_sql(target_relation, tmp_relation,  dest_columns)) %}
  {% elif strategy == 'merge' %}
    {% do return(teradata__get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns,incremental_predicates)) %}
  {% elif strategy == 'valid_history' %}
    {% do return(teradata__get_incremental_valid_history_sql(target_relation, tmp_relation, unique_key, valid_period, valid_from, valid_to, use_valid_to_time, history_column_in_target, resolve_conflicts)) %}
  {% else %}
    {% do exceptions.raise_compiler_error("Invalid Strategy") %}
  {% endif %}
{% endmacro %}