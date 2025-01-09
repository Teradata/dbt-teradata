{% materialization snapshot, adapter='teradata' %}

  -- calling the macro set_query_band() which will set the query_band for this materialization as per the user_configuration
  {% do set_query_band() %}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  -- grab current tables grants config for comparision later on
  {%- set grant_config = config.get('grants') -%}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {# The model['config'] parameter below is no longer used, but passing anyway for compatibility #}
  {# It was a dictionary of config, instead of the config object from the context #}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", model['config'], target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

      {% call statement('main') %}
          {{ final_sql }}
      {% endcall %}

  {% else %}

      {% do adapter.drop_relation(make_temp_relation(target_relation)) %}

      {% set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() %}

      {{ adapter.assert_valid_snapshot_target_given_strategy(target_relation, columns, strategy) }}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}
      
      {% set remove_columns = ['dbt_change_type', 'DBT_CHANGE_TYPE', 'dbt_unique_key', 'DBT_UNIQUE_KEY'] %}
      {% if unique_key | is_list %}
          {% for key in strategy.unique_key %}
              {{ remove_columns.append('dbt_unique_key_' + loop.index|string) }}
              {{ remove_columns.append('DBT_UNIQUE_KEY_' + loop.index|string) }}
          {% endfor %}
      {% endif %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'in', remove_columns)
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'in', remove_columns)
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      -- Use separate DELETE + UPDATE + INSERT statements instead of the MERGE statement
      {%- if strategy.invalidate_hard_deletes %}
      {% set final_sql_delete = teradata__snapshot_merge_sql_delete(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}
      {%- endif %}

      {% set final_sql_update = teradata__snapshot_merge_sql_update(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

      {% set final_sql_insert = teradata__snapshot_merge_sql_insert(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

      {%- if strategy.invalidate_hard_deletes %}
      {% call statement('main') %}
          {{ final_sql_delete }}
      {% endcall %}
      {%- endif %}

      {% call statement('main') %}
          {{ final_sql_update }}
      {% endcall %}

      {% call statement('main') %}
          {{ final_sql_insert }}
      {% endcall %}

  {% endif %}

  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode=False) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if not target_relation_exists %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
