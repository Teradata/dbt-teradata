{#
  OTF (Open Table Format) incremental materialization support.

  Handles incremental builds for Iceberg/Delta Lake tables via Teradata's
  native OTF support using DATALAKE 3-part naming.

  Supported strategies:
    - append : INSERT new rows into the existing OTF table

  Staging uses a REGULAR Teradata table (__dbt_tmp suffix) in the dbt target
  schema.  We cannot use create_table_as() because teradata__create_table_as
  checks config.get('catalog_name') and routes to teradata__create_otf_table_as
  whenever catalog_name is set — regardless of the relation type.  An OTF
  staging table is invisible to DBC.ColumnsV, so get_columns_in_relation()
  returns an empty list and the INSERT fails with Error 3706.  Instead we issue
  native Teradata CREATE TABLE AS ... WITH NO DATA / INSERT SQL directly,
  bypassing all config-based routing.  The staging table is explicitly dropped
  after use.
#}


{% macro teradata__validate_get_otf_incremental_strategy(config) %}
  {#-- Validate and return the incremental strategy for OTF models.
       Only 'append' is currently supported. --#}
  {%- set strategy = config.get("incremental_strategy") or "append" -%}

  {% if strategy not in ['append'] %}
    {{ exceptions.raise_compiler_error(
        "Invalid incremental strategy '" ~ strategy ~ "' for OTF models. "
        "Only 'append' is currently supported for OTF (Iceberg/Delta Lake) tables. "
        "'delete+insert', 'merge', 'valid_history', and 'microbatch' are not yet supported."
    ) }}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}


{% macro teradata__get_otf_incremental_append_sql(otf_relation_name, tmp_relation, dest_columns) %}
  {#-- Append strategy: INSERT INTO OTF table from staging table.
       OTF tables require positional inserts (no column list on the target);
       specifying column names raises Error 3706 "OTF tables do not support
       non-positional inserts". --#}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  insert into {{ otf_relation_name }}
      select {{ dest_cols_csv }}
      from {{ tmp_relation }}
  ;
{% endmacro %}


{% macro teradata__otf_reconcile_schema(on_schema_change, otf_relation_name, target_columns, staging_columns) %}
  {#-- on_schema_change reconciliation for OTF incremental models (Phase 1).

       Args:
         on_schema_change : 'ignore' | 'fail' | 'append_new_columns'
         otf_relation_name: rendered 3-part OTF name
         target_columns   : list of column NAME strings currently on the OTF table
                            (from adapter.get_otf_columns_in_relation)
         staging_columns  : list of Column objects from the staging table -- the
                            incoming schema, carrying name + DDL type

       Returns:
         none  -> caller should use the default positional append
         list  -> positional SELECT expressions aligned to the final OTF column
                  order (used when columns were added under append_new_columns)

       Comparison is case-insensitive. New columns are added one ALTER at a time
       because OTF cannot combine ALTER operations and External OTF disallows
       multi-statement requests. --#}

  {%- set target_lower = [] -%}
  {%- for t in target_columns -%}{%- do target_lower.append(t | lower) -%}{%- endfor -%}
  {%- set staging_lower = [] -%}
  {%- for s in staging_columns -%}{%- do staging_lower.append(s.name | lower) -%}{%- endfor -%}

  {#-- new_columns: in staging but not in target; removed_columns: target not in staging --#}
  {%- set new_columns = [] -%}
  {%- for s in staging_columns -%}
    {%- if (s.name | lower) not in target_lower -%}{%- do new_columns.append(s) -%}{%- endif -%}
  {%- endfor -%}
  {%- set removed_columns = [] -%}
  {%- for t in target_columns -%}
    {%- if (t | lower) not in staging_lower -%}{%- do removed_columns.append(t) -%}{%- endif -%}
  {%- endfor -%}

  {%- if on_schema_change == 'ignore' -%}
    {{ return(none) }}

  {%- elif on_schema_change == 'fail' -%}
    {%- if (new_columns | length > 0) or (removed_columns | length > 0) -%}
      {{ exceptions.raise_compiler_error(
          "Schema change detected on OTF incremental model with on_schema_change='fail'. "
          ~ "Columns added in source: [" ~ (new_columns | map(attribute='name') | join(', ')) ~ "]; "
          ~ "columns missing from source: [" ~ (removed_columns | join(', ')) ~ "]. "
          ~ "Reconcile the model, switch to on_schema_change='append_new_columns', "
          ~ "or run with --full-refresh."
      ) }}
    {%- endif -%}
    {{ return(none) }}

  {%- elif on_schema_change == 'append_new_columns' -%}
    {%- if (new_columns | length == 0) and (removed_columns | length == 0) -%}
      {#-- No drift -- the default positional append is correct. --#}
      {{ return(none) }}
    {%- endif -%}

    {#-- Add each new source column with its own ALTER. New columns land at the
         end of the OTF table. Type is sourced from the staging column. --#}
    {%- for col in new_columns -%}
      {% call statement('otf_add_column_' ~ loop.index, auto_begin=False) -%}
        ALTER TABLE {{ otf_relation_name }} ADD {{ adapter.quote(col.name) }} {{ col.data_type }};
      {%- endcall %}
    {%- endfor -%}

    {#-- Final OTF column order = existing target columns (original order) then the
         newly added columns. Build a positional SELECT in that exact order so the
         column-list-less OTF INSERT aligns. A target column no longer present in
         the source (append_new_columns keeps it) gets NULL. --#}
    {%- set final_order = target_columns + (new_columns | map(attribute='name') | list) -%}
    {%- set exprs = [] -%}
    {%- for cname in final_order -%}
      {%- set ns = namespace(match=none) -%}
      {%- for s in staging_columns -%}
        {%- if (s.name | lower) == (cname | lower) -%}{%- set ns.match = s -%}{%- endif -%}
      {%- endfor -%}
      {%- if ns.match is not none -%}
        {%- do exprs.append(adapter.quote(ns.match.name)) -%}
      {%- else -%}
        {%- do exprs.append('NULL') -%}
      {%- endif -%}
    {%- endfor -%}
    {{ return(exprs) }}
  {%- endif -%}
{% endmacro %}


{% macro teradata__incremental_otf(catalog_name, sql) %}
  {#-- Main entry point for OTF incremental materialization. --#}

  {#-- Validate strategy (currently only 'append' is supported) --#}
  {%- do teradata__validate_get_otf_incremental_strategy(config) -%}

  {#-- Guardrails: reject Teradata-native options that do not apply to OTF --#}
  {%- set unsupported = [] -%}
  {%- if config.get('table_kind') -%}{%- do unsupported.append('table_kind') -%}{%- endif -%}
  {%- if config.get('table_option') -%}{%- do unsupported.append('table_option') -%}{%- endif -%}
  {%- if config.get('with_statistics', default=False) | as_bool -%}{%- do unsupported.append('with_statistics') -%}{%- endif -%}
  {%- if config.get('index') -%}{%- do unsupported.append('index') -%}{%- endif -%}
  {%- if unsupported | length > 0 -%}
    {{ exceptions.raise_compiler_error(
        "The following config option(s) are not supported with catalog_name (OTF): "
        ~ unsupported | join(', ')
    ) }}
  {%- endif -%}

  {#-- on_schema_change handling for OTF incremental (Phase 1).
       Supported: 'ignore' (default), 'fail', 'append_new_columns'.

       'sync_all_columns' is intentionally NOT supported: it must also
       synchronize column TYPE changes, but OTF/Iceberg only permits a narrow
       set of safe promotions (e.g. INTEGER->BIGINT). Common changes such as
       VARCHAR length changes fail at ALTER ... MODIFY with Error 7825, so the
       'sync all types' contract cannot be honored reliably. It also performs
       destructive, irreversible column DROPs (OTF has no rollback).
       'append_new_columns' avoids both: it is ADD-only, never retypes existing
       columns, and is non-destructive.
       The actual reconciliation runs in the incremental (append) branch below
       via teradata__otf_reconcile_schema(). --#}
  {%- set on_schema_change = config.get('on_schema_change', 'ignore') or 'ignore' -%}
  {%- set _otf_supported_osc = ['ignore', 'fail', 'append_new_columns'] -%}
  {%- if on_schema_change == 'sync_all_columns' -%}
    {{ exceptions.raise_compiler_error(
        "on_schema_change='sync_all_columns' is not yet supported for OTF incremental models. "
        ~ "Use 'append_new_columns' to add new source columns, 'fail' to error on drift, "
        ~ "or run with --full-refresh."
    ) }}
  {%- elif on_schema_change not in _otf_supported_osc -%}
    {{ exceptions.raise_compiler_error(
        "Invalid on_schema_change='" ~ on_schema_change ~ "' for OTF incremental models. "
        ~ "Supported values: " ~ (_otf_supported_osc | join(', ')) ~ "."
    ) }}
  {%- endif -%}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config is not none and contract_config.enforced -%}
    {{ exceptions.raise_compiler_error(
        "Model contracts (contract.enforced=true) are not yet supported with catalog_name (OTF)."
    ) }}
  {%- endif -%}

  {#-- on_schema_change is not supported for OTF incremental models: the append
       strategy does not reconcile target/source schemas, and the other values
       ('fail', 'append_new_columns', 'sync_all_columns') cannot be honoured.
       Users must run with --full-refresh to apply schema changes.
       NOTE: checked after contract.enforced so that contract violations surface
       a clear "contract" error even when dbt core requires on_schema_change='fail'
       for contracted incremental models. --#}
  {%- set on_schema_change = config.get('on_schema_change', none) -%}
  {%- if on_schema_change is not none and on_schema_change != 'ignore' -%}
    {{ exceptions.raise_compiler_error(
        "on_schema_change='" ~ on_schema_change ~ "' is not supported for OTF incremental models. "
        ~ "Use --full-refresh to apply schema changes to an OTF table."
    ) }}
  {%- endif -%}

  {#-- Resolve target and check existence --#}
  {%- set catalog_integration = adapter.get_catalog_integration(catalog_name) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {#-- OTF tables are not registered in DBC.TablesV under the dbt target schema,
       so load_relation(this) always returns none and would cause every run to
       take the full-refresh (CREATE) path.  Instead, probe via SAMPLE 0 against
       the 3-part DATALAKE name: succeeds when the table exists, raises
       Teradata Error 7825 (ICEBERG_EXPORT "Table does not exist") when it does
       not.  The adapter method catches the exception and returns True/False. --#}
  {%- set otf_exists = adapter.otf_relation_exists(
      catalog_integration.datalake_name,
      catalog_integration.otf_database,
      target_relation.identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if not otf_exists or should_full_refresh() %}
    {#-- FIRST RUN or --full-refresh: create OTF table from scratch
         (DROP + CREATE AS via the existing table-materialization macro). --#}
    {{ teradata__create_otf_table_as(target_relation, sql, catalog_name) }}

  {% else %}
    {#-- INCREMENTAL RUN --#}

    {#-- Step 1: Create a REGULAR (non-OTF) staging table in the dbt target schema.
         We cannot call create_table_as(True, tmp_relation, sql) because
         teradata__create_table_as checks config.get('catalog_name') and always
         routes to teradata__create_otf_table_as when catalog_name is set —
         regardless of the relation type.  An OTF staging table is invisible to
         DBC.ColumnsV, so adapter.get_columns_in_relation() returns [] and the
         INSERT into the OTF target fails with Error 3706 (empty column list).
         Instead, issue native Teradata DDL directly into target.schema (a regular
         Teradata database) to bypass config-based routing entirely.  The staging
         data is just SQL rows; Parquet serialisation only happens at the final
         INSERT INTO the OTF target. --#}
    {%- set tmp_relation = make_temp_relation(api.Relation.create(
        schema=target.schema,
        identifier=target_relation.identifier,
        type='table'
    )) -%}
    {% call statement('drop_preexisting_staging', auto_begin=False) %}
      DROP TABLE /*+ IF EXISTS */ {{ tmp_relation }};
    {% endcall %}
    {% call statement('create_staging', auto_begin=False) %}
      CREATE MULTISET TABLE {{ tmp_relation }} AS ({{ sql }}) WITH NO DATA;
    {% endcall %}
    {% call statement('populate_staging') %}
      INSERT INTO {{ tmp_relation }}
      {{ sql }}
      ;
    {% endcall %}

    {#-- Step 2: Get destination columns from the staging table.
         The staging table is a regular Teradata table so DBC.ColumnsV has its schema. --#}
    {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}

    {#-- Step 3: Build the 3-part OTF relation name --#}
    {%- set otf_relation_name = teradata__build_otf_relation_name(catalog_integration, target_relation.identifier) -%}

    {#-- Step 3a: on_schema_change reconciliation.
         Read the OTF target's current columns via SAMPLE 0 metadata (DBC.ColumnsV
         does not see OTF tables), diff against the staging schema, and for
         'append_new_columns' issue one ALTER TABLE ADD per new column. Returns
         the positional SELECT expression list aligned to the final OTF column
         order, or none to use the default positional append. --#}
    {%- set target_otf_columns = adapter.get_otf_columns_in_relation(
        catalog_integration.datalake_name,
        catalog_integration.otf_database,
        target_relation.identifier) -%}
    {%- set insert_exprs = teradata__otf_reconcile_schema(
        on_schema_change, otf_relation_name, target_otf_columns, dest_columns) -%}

    {% call statement('main') %}
      {%- if insert_exprs is none -%}
        {{ teradata__get_otf_incremental_append_sql(otf_relation_name, tmp_relation, dest_columns) }}
      {%- else -%}
        insert into {{ otf_relation_name }}
            select {{ insert_exprs | join(', ') }}
            from {{ tmp_relation }}
        ;
      {%- endif -%}
    {% endcall %}

    {#-- Step 4: Cleanup staging table --#}
    {% call statement('drop_staging', auto_begin=False) %}
      DROP TABLE /*+ IF EXISTS */ {{ tmp_relation }};
    {% endcall %}

  {% endif %}

  {#-- Grants warning — not supported on OTF --#}
  {%- if config.get('grants') -%}
    {{ exceptions.warn("grants config is ignored for OTF models — Teradata does not support GRANT on DATALAKE tables.") }}
  {%- endif -%}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {% do adapter.cache_added(target_relation) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}
