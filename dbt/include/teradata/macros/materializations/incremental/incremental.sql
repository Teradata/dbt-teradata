

{% materialization incremental, adapter='teradata' -%}

-- calling the macro set_query_band() which will set the query_band for this materialization as per the user_configuration
{% do set_query_band() %}

{% set unique_key = config.get('unique_key') %}

-- Start: Below are the configuration options for the valid_history strategy
{% set valid_period = config.get('valid_period', none) %}
{% set valid_from = config.get('valid_from', none) %}
{% set valid_to = config.get('valid_to', none) %}
{% set use_valid_to_time = config.get('use_valid_to_time', default='no') %}
{% set resolve_conflicts = config.get('resolve_conflicts', default='yes') %}
{% set history_column_in_target = config.get('history_column_in_target', none) %}
-- End: Above are the configuration options for the valid_history strategy

{% set target_relation = this.incorporate(type='table') %}
{% set existing_relation = load_relation(this) %}
{% set tmp_relation = make_temp_relation(this) %}

-- {#-- Validate early so we don't run SQL if the strategy is invalid --#}
{% set strategy = teradata__validate_get_incremental_strategy(config) %}

{% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}

{% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

{%- set preexisting_tmp_relation = load_cached_relation(tmp_relation) -%}
{{ drop_relation_if_exists(preexisting_tmp_relation) }}

{{ run_hooks(pre_hooks, inside_transaction=False) }}

-- `BEGIN` happens here:
{{ run_hooks(pre_hooks, inside_transaction=True) }}

{% set to_drop = [] %}
{% if existing_relation is none %}
   {% set build_sql = create_table_as(False, target_relation, sql) %}
{% elif existing_relation.is_view or should_full_refresh() %}
   {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
   {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
   {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
   {% do adapter.drop_relation(backup_relation) %}

   {% do adapter.rename_relation(target_relation, backup_relation) %}
   {% set build_sql = create_table_as(False, target_relation, sql) %}
   {% do to_drop.append(backup_relation) %}
{% else %}
   {% set tmp_relation = make_temp_relation(target_relation) %}
   {% do run_query(create_table_as(True, tmp_relation, sql)) %}
   {% do adapter.expand_target_column_types(
          from_relation=tmp_relation,
          to_relation=target_relation) %}
   

   {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
        {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {% endif %}


   {% set build_sql = teradata__get_incremental_sql(strategy, target_relation, tmp_relation, unique_key, dest_columns,incremental_predicates,
   valid_period, valid_from, valid_to, use_valid_to_time, history_column_in_target, resolve_conflicts) %}


   {% do to_drop.append(tmp_relation) %}
{% endif %}

{% call statement("main") %}
   {{ build_sql }}
{% endcall %}

-- apply grants
{%- set grant_config = config.get('grants') -%}
{% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
{% do apply_grants(target_relation, grant_config, should_revoke) %}

{% do persist_docs(target_relation, model) %}

{{ run_hooks(post_hooks, inside_transaction=True) }}


-- `COMMIT` happens here
{% do adapter.commit() %}

{% for rel in to_drop %}
   {% do adapter.drop_relation(rel) %}
{% endfor %}

{{ run_hooks(post_hooks, inside_transaction=False) }}

{{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
