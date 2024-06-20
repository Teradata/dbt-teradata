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