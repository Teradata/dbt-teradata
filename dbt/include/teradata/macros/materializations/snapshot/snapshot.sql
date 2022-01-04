
{% macro teradata__snapshot_string_as_time(timestamp) -%}
    {%- set result = "TO_TIMESTAMP_TZ('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select snapshot.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }} as snapshot

    ),

    insertions_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and snapshotted_data.dbt_valid_to is null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_valid_to is null
        and (
            {{ strategy.row_changed }}
        )
    )

    select * from insertions
    union all
    select * from updates

{%- endmacro %}

{% macro build_snapshot_table(strategy, sql) %}

    select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) as sbq

{% endmacro %}

{% materialization snapshot, adapter='teradata' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

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
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

      {% call statement('main') %}
          {{ final_sql }}
      {% endcall %}

  {% else %}

      {% do adapter.drop_relation(make_temp_relation(target_relation)) %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      -- Use seperate UPDATE + INSERT statements instead of the MERGE statement
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

      {% call statement('main') %}
          {{ final_sql_update }}
      {% endcall %}

      {% call statement('main') %}
          {{ final_sql_insert }}
      {% endcall %}

  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro snapshot_check_all_get_existing_columns(node, target_exists) -%}
    {%- set query_columns = get_columns_in_query(node['compiled_sql']) -%}
    {%- if not target_exists -%}
        {# no table yet -> return whatever the query does #}
        {{ return([false, query_columns]) }}
    {%- endif -%}
    {# handle any schema changes #}
    {%- set target_table = node.get('alias', node.get('name')) -%}
    {%- set target_relation = adapter.get_relation(database=None, schema=node.schema, identifier=target_table) -%}
    {%- set existing_cols = get_columns_in_query('select * from ' ~ target_relation) -%}
    {%- set ns = namespace() -%} {# handle for-loop scoping with a namespace #}
    {%- set ns.column_added = false -%}

    {%- set intersection = [] -%}
    {%- for col in query_columns -%}
        {%- if col in existing_cols -%}
            {%- do intersection.append(col) -%}
        {%- else -%}
            {% set ns.column_added = true %}
        {%- endif -%}
    {%- endfor -%}
    {{ return([ns.column_added, intersection]) }}
{%- endmacro %}

{#-- This macro is copied varbatim from dbt-core. The only delta is that != operator is replaced with <> #}
{% macro snapshot_check_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}

    {% set select_current_time -%}
        select {{ snapshot_get_time() }} as snapshot_start
    {%- endset %}

    {#-- don't access the column by name, to avoid dealing with casing issues on snowflake #}
    {%- set now = run_query(select_current_time)[0][0] -%}
    {% if now is none or now is undefined -%}
        {%- do exceptions.raise_compiler_error('Could not get a snapshot start time from the database') -%}
    {%- endif %}
    {% set updated_at = snapshot_string_as_time(now) %}

    {% set column_added = false %}

    {% if check_cols_config == 'all' %}
        {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        TRUE
    {%- else -%}
    {%- for col in check_cols -%}
        {{ snapshotted_rel }}.{{ col }} <> {{ current_rel }}.{{ col }}
        or
        (
            (({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null))
            or
            ((not {{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null))
        )
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes
    }) %}
{% endmacro %}
