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
  {%- set strategy = config.get("incremental_strategy", "append") -%}

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
    {#-- No drift: still build a name-aligned positional SELECT so a reorder of
         existing columns in the model SELECT cannot misalign the OTF insert. --#}
    {%- set exprs = [] -%}
    {%- for cname in target_columns -%}
      {%- set ns = namespace(match=none) -%}
      {%- for s in staging_columns -%}
        {%- if (s.name | lower) == (cname | lower) -%}{%- set ns.match = s -%}{%- endif -%}
      {%- endfor -%}
      {%- do exprs.append(adapter.quote(ns.match.name)) -%}
    {%- endfor -%}
    {{ return(exprs) }}

  {%- elif on_schema_change == 'append_new_columns' -%}
    {#-- Add each new source column with its own ALTER (this loop is a no-op when
         there are no new columns). New columns land at the end of the OTF table.
         Type is sourced from the staging column. --#}
    {%- for col in new_columns -%}
      {% call statement('otf_add_column_' ~ loop.index, auto_begin=False) -%}
        ALTER TABLE {{ otf_relation_name }} ADD {{ adapter.quote(col.name) }} {{ col.data_type }};
      {%- endcall %}
    {%- endfor -%}

    {#-- Always build a positional SELECT aligned to the final OTF column order --
         existing target columns in their current order, then the newly added
         columns -- even when the column set did not change. OTF inserts are
         column-list-less/positional, so aligning by name here makes a *reorder*
         of existing columns in the model SELECT safe (it cannot misalign data).
         A target column no longer present in the source (append_new_columns keeps
         such columns) is set to NULL. --#}
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


{% macro teradata__otf_sync_all_columns(otf_relation_name, target_columns, staging_columns) %}
  {#-- sync_all_columns reconciliation for OTF incremental models (best-effort).

       Args:
         otf_relation_name: rendered 3-part OTF name
         target_columns   : list of {name, otf_type} from get_otf_column_types
                            (names lower-cased; otf_type is the Iceberg type)
         staging_columns  : list of Column objects -- the incoming schema

       Makes the OTF table match the source: ADD new columns, DROP columns no
       longer in the source (destructive), and MODIFY columns whose OTF/Iceberg
       type changed *and* is a promotion OTF allows. A type change OTF cannot
       apply in place raises a clear error (use --full-refresh).

       Type comparison is at OTF/Iceberg granularity, so VARCHAR length and
       SMALLINT-vs-INTEGER differences are not treated as changes. Each change is
       its own ALTER (OTF cannot combine alter ops / no multi-statement requests;
       no rollback if one fails midway).

       Returns the positional SELECT expressions aligned to the final OTF column
       order (surviving target columns in order, then newly added columns). --#}

  {%- set target_names = [] -%}
  {%- set target_type_by_name = {} -%}
  {%- for c in target_columns -%}
    {%- do target_names.append(c.name) -%}
    {%- do target_type_by_name.update({c.name | lower: c.otf_type}) -%}
  {%- endfor -%}
  {%- set target_lower = [] -%}
  {%- for n in target_names -%}{%- do target_lower.append(n | lower) -%}{%- endfor -%}
  {%- set staging_lower = [] -%}
  {%- for s in staging_columns -%}{%- do staging_lower.append(s.name | lower) -%}{%- endfor -%}

  {#-- Compute the full diff FIRST, without running any DDL: columns to add, to
       drop, and to retype. Type changes are validated here so an unsupported one
       raises BEFORE any ALTER executes -- otherwise a partial ADD/DROP could be
       left behind, and OTF has no rollback. --#}
  {%- set new_columns = [] -%}
  {%- for s in staging_columns -%}
    {%- if (s.name | lower) not in target_lower -%}{%- do new_columns.append(s) -%}{%- endif -%}
  {%- endfor -%}

  {%- set removed_columns = [] -%}
  {%- for n in target_names -%}
    {%- if (n | lower) not in staging_lower -%}{%- do removed_columns.append(n) -%}{%- endif -%}
  {%- endfor -%}

  {%- set modify_columns = [] -%}
  {%- for s in staging_columns -%}
    {%- if (s.name | lower) in target_lower -%}
      {%- set old_type = target_type_by_name[s.name | lower] -%}
      {%- set new_type = adapter.teradata_type_to_otf_type(s.data_type) -%}
      {%- if old_type != new_type -%}
        {%- if adapter.otf_type_promotion_allowed(old_type, new_type) -%}
          {%- do modify_columns.append(s) -%}
        {%- else -%}
          {{ exceptions.raise_compiler_error(
              "on_schema_change='sync_all_columns': column '" ~ s.name ~ "' changed type from '"
              ~ old_type ~ "' to '" ~ new_type ~ "', which OTF/Iceberg cannot apply in place. "
              ~ "Run the model with --full-refresh to rebuild it."
          ) }}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}

  {#-- Apply the changes, one ALTER each (OTF cannot combine alter ops / no
       multi-statement requests; no rollback if one fails midway). DROP is
       destructive -- the column and its data are removed. --#}
  {%- for col in new_columns -%}
    {% call statement('otf_sync_add_' ~ loop.index, auto_begin=False) -%}
      ALTER TABLE {{ otf_relation_name }} ADD {{ adapter.quote(col.name) }} {{ col.data_type }};
    {%- endcall %}
  {%- endfor -%}
  {%- for col in modify_columns -%}
    {% call statement('otf_sync_modify_' ~ loop.index, auto_begin=False) -%}
      ALTER TABLE {{ otf_relation_name }} MODIFY {{ adapter.quote(col.name) }} {{ col.data_type }};
    {%- endcall %}
  {%- endfor -%}
  {%- for col in removed_columns -%}
    {% call statement('otf_sync_drop_' ~ loop.index, auto_begin=False) -%}
      ALTER TABLE {{ otf_relation_name }} DROP {{ adapter.quote(col) }};
    {%- endcall %}
  {%- endfor -%}

  {#-- Final OTF column order = surviving target columns (original order, minus
         dropped) then the newly added columns. Build a positional SELECT in that
         order; every column is present in the source (dropped ones are gone). --#}
  {%- set final_order = [] -%}
  {%- for n in target_names -%}
    {%- if (n | lower) in staging_lower -%}{%- do final_order.append(n) -%}{%- endif -%}
  {%- endfor -%}
  {%- for col in new_columns -%}{%- do final_order.append(col.name) -%}{%- endfor -%}

  {%- set exprs = [] -%}
  {%- for cname in final_order -%}
    {%- set ns = namespace(match=none) -%}
    {%- for s in staging_columns -%}
      {%- if (s.name | lower) == (cname | lower) -%}{%- set ns.match = s -%}{%- endif -%}
    {%- endfor -%}
    {%- do exprs.append(adapter.quote(ns.match.name)) -%}
  {%- endfor -%}
  {{ return(exprs) }}
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

  {#-- on_schema_change handling for OTF incremental.
       Supported: 'ignore' (default), 'fail', 'append_new_columns', 'sync_all_columns'.

       'sync_all_columns' is best-effort on OTF: it ADDs new source columns,
       DROPs columns missing from the source (destructive -- OTF has no
       rollback), and applies column TYPE changes that OTF/Iceberg permits
       (e.g. int->long, decimal precision widening). A type change OTF cannot
       apply in place raises a clear error directing the user to --full-refresh.
       Type comparison is done at OTF/Iceberg type granularity (via HELP TABLE
       'OTF Type'), so VARCHAR length and SMALLINT-vs-INTEGER -- which OTF does
       not preserve -- are never treated as changes.
       Reconciliation runs in the incremental (append) branch below via
       teradata__otf_reconcile_schema() (ignore/fail/append_new_columns) or
       teradata__otf_sync_all_columns() (sync_all_columns). --#}
  {%- set on_schema_change = config.get('on_schema_change', 'ignore') or 'ignore' -%}
  {%- set _otf_supported_osc = ['ignore', 'fail', 'append_new_columns', 'sync_all_columns'] -%}
  {%- if on_schema_change not in _otf_supported_osc -%}
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
         For the default 'ignore' there is nothing to reconcile, so we skip the
         OTF column probe entirely (it is an extra SAMPLE 0 round-trip against
         the DATALAKE). For 'fail'/'append_new_columns', read the OTF target's
         current columns via SAMPLE 0 metadata (DBC.ColumnsV does not see OTF
         tables), diff against the staging schema, and for 'append_new_columns'
         issue one ALTER TABLE ADD per new column. teradata__otf_reconcile_schema
         returns the positional SELECT expression list aligned to the final OTF
         column order, or none to use the default positional append. --#}
    {%- if on_schema_change == 'ignore' -%}
      {%- set insert_exprs = none -%}
    {%- elif on_schema_change == 'sync_all_columns' -%}
      {#-- sync needs column TYPES, read from HELP TABLE (OTF Type). --#}
      {%- set target_typed_columns = adapter.get_otf_column_types(
          catalog_integration.datalake_name,
          catalog_integration.otf_database,
          target_relation.identifier) -%}
      {%- set insert_exprs = teradata__otf_sync_all_columns(
          otf_relation_name, target_typed_columns, dest_columns) -%}
    {%- else -%}
      {#-- fail / append_new_columns need column NAMES only. --#}
      {%- set target_otf_columns = adapter.get_otf_columns_in_relation(
          catalog_integration.datalake_name,
          catalog_integration.otf_database,
          target_relation.identifier) -%}
      {%- set insert_exprs = teradata__otf_reconcile_schema(
          on_schema_change, otf_relation_name, target_otf_columns, dest_columns) -%}
    {%- endif -%}

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
