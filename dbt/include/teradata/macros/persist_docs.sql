{#
  persist_docs support for Teradata (IDE-26225).

  Writes model / column `description:` YAML into the Teradata catalog as native
  object comments using COMMENT ON TABLE|VIEW|FUNCTION|COLUMN ... AS '...'.

  Teradata specifics handled here:
    * Comment string literals are escaped by doubling single quotes.
    * Comments are capped at 255 characters (Teradata limit) -> truncated with a warning.
    * ANSI mode forbids multi-statement DDL, so each column comment is issued in its
      own statement() block rather than a single semicolon-joined batch.
    * OTF / Iceberg relations (config.catalog_name set) do not support COMMENT ON and
      are skipped gracefully.
    * Change detection: unchanged comments issue no DDL on re-run.
    * Function (UDF) relations only support the relation-level comment
      (COMMENT ON FUNCTION) — Teradata has no per-argument comment DDL, so
      persist_docs `columns` config is skipped with a warning for functions.
#}

{#-- Truncates `comment` to the configured/Teradata comment limit, returning the plain
     (unescaped) string. Shared by teradata_escape_comment (DDL emission) and
     teradata__persist_docs / teradata__alter_column_comment (change detection), so that
     both sides of the "has this comment changed?" comparison are truncated the same way.
     Without this, a description longer than the limit would never match what's actually
     stored (which is always truncated), so COMMENT ON DDL would be re-issued on every
     run for over-length comments on persistent relations (incremental/function), rather
     than only when the comment is genuinely new/changed. `warn` suppresses the
     truncation warning for the change-detection call so it isn't logged twice (once
     during detection, once when the DDL is actually built) when a comment is re-issued. --#}
{%- macro teradata_truncate_comment(comment, warn=True) -%}
  {%- if comment is not string -%}
    {%- do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) -%}
  {%- endif -%}
  {#-- Teradata stores comments in DBC ...CommentString (VARCHAR(255)); overridable via var.
      `int(255)` falls back to 255 for null/non-numeric values, and a non-positive
      override is ignored (also falls back to 255) so we never emit empty/garbled comments. --#}
  {%- set max_len = var("teradata_max_comment_length", 255) | int(255) -%}
  {%- if max_len <= 0 -%}
    {%- set max_len = 255 -%}
  {%- endif -%}
  {%- if comment | length > max_len -%}
    {%- if warn -%}
      {{ exceptions.warn("Comment exceeds the configured Teradata limit of " ~ max_len ~ " characters; truncating: " ~ comment[:40] ~ "...") }}
    {%- endif -%}
    {%- set comment = comment[:max_len] -%}
  {%- endif -%}
  {{- comment -}}
{%- endmacro -%}


{%- macro teradata_escape_comment(comment) -%}
  {%- set truncated = teradata_truncate_comment(comment) -%}
  {%- set escaped = truncated | replace("'", "''") -%}
  {{- "'" ~ escaped ~ "'" -}}
{%- endmacro -%}


{% macro teradata__alter_relation_comment(relation, comment) -%}
  {%- set escaped = teradata_escape_comment(comment) -%}
  {%- if relation.type == 'view' -%}
    comment on view {{ relation }} as {{ escaped }}
  {%- elif relation.type == 'function' -%}
    comment on function {{ relation }} as {{ escaped }}
  {%- else -%}
    comment on table {{ relation }} as {{ escaped }}
  {%- endif -%}
{%- endmacro %}


{#-- Fetch the current relation-level comment for change detection. Returns none if absent. --#}
{% macro teradata__get_relation_comment(relation) -%}
  {% call statement('get_relation_comment', fetch_result=True) %}
    SELECT CommentString FROM DBC.TablesV
    WHERE DatabaseName = '{{ relation.schema }}' (NOT CASESPECIFIC)
      AND TableName = '{{ relation.identifier }}' (NOT CASESPECIFIC)
  {% endcall %}
  {%- set result = load_result('get_relation_comment').table -%}
  {%- if result and result.rows | length > 0 -%}
    {%- set value = result.columns['CommentString'].values()[0] -%}
    {{ return(value | trim if value is not none else none) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}


{#-- Self-contained column validation (does not depend on the global validate_doc_columns,
     which only exists in dbt-adapters >= 1.22.10). Warns about documented columns absent
     from the database and returns only the columns that exist. Honors the per-column
     `quote` flag: quoted identifiers compare case-sensitively, unquoted case-insensitively. --#}
{% macro teradata__validate_doc_columns(relation, column_dict, existing_column_names) -%}
  {%- set existing_lower = existing_column_names | map("lower") | list -%}
  {%- set missing = [] -%}
  {%- set filtered = {} -%}
  {%- for col_name in column_dict -%}
    {%- if column_dict[col_name]['quote'] -%}
      {%- set present = col_name in existing_column_names -%}
    {%- else -%}
      {%- set present = col_name | lower in existing_lower -%}
    {%- endif -%}
    {%- if present -%}
      {%- do filtered.update({col_name: column_dict[col_name]}) -%}
    {%- else -%}
      {%- do missing.append(col_name) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if missing | length > 0 -%}
    {{ exceptions.warn("In relation " ~ relation.render() ~ ": The following columns are specified in the schema but are not present in the database: " ~ missing | join(", ")) }}
  {%- endif -%}
  {{ return(filtered) }}
{%- endmacro %}


{#-- Existing column comments keyed by column name, read straight from DBC.ColumnsV for
     the *real* relation. This is the correct source for change detection: for views under
     use_qvci=False, adapter.get_columns_in_relation reads a comment-less temp table, so
     relying on it would report every column as having no comment. Querying DBC.ColumnsV
     directly also avoids creating/dropping that temp table for views. --#}
{% macro teradata__get_column_comments(relation) -%}
  {% call statement('get_column_comments', fetch_result=True) %}
    SELECT ColumnName, CommentString FROM DBC.ColumnsV
    WHERE DatabaseName = '{{ relation.schema }}' (NOT CASESPECIFIC)
      AND TableName = '{{ relation.identifier }}' (NOT CASESPECIFIC)
  {% endcall %}
  {%- set result = load_result('get_column_comments').table -%}
  {%- set comments = {} -%}
  {%- if result -%}
    {%- for row in result.rows -%}
      {%- do comments.update({row[0]: row[1]}) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(comments) }}
{%- endmacro %}


{#-- Issues one COMMENT ON COLUMN statement per changed column (ANSI-safe: no batching).
     column_dict has already been filtered to existing columns. `existing_comments` is a
     {column_name: comment_string} map from teradata__get_column_comments. Note: for view
     materializations REPLACE VIEW drops column comments, so on a view re-run the map is
     empty and the comments are (correctly) re-applied. --#}
{% macro teradata__alter_column_comment(relation, column_dict, existing_comments) -%}
  {#-- Build a lower-cased lookup so change detection mirrors the quote semantics: quoted
      identifiers match case-sensitively, unquoted ones case-insensitively (a documented
      `ID` still matches a physical `id`, so its comment is not re-issued every run). --#}
  {%- set existing_by_lower = {} -%}
  {%- for name, cmt in existing_comments.items() -%}
    {%- do existing_by_lower.update({name | lower: cmt}) -%}
  {%- endfor -%}
  {%- for column_name in column_dict -%}
    {%- set desc = column_dict[column_name]['description'] -%}
    {%- set quoted = column_dict[column_name]['quote'] -%}
    {%- set rendered_col = adapter.quote(column_name) if quoted else column_name -%}
    {%- set existing_raw = existing_comments.get(column_name) if quoted else existing_by_lower.get(column_name | lower) -%}
    {%- set existing_comment = (existing_raw | trim) if existing_raw is not none else none -%}
    {#-- Compare trimmed, truncated forms: YAML block scalars carry a trailing newline that
        Teradata does not store, and `existing_comment` (read back from the catalog) is
        always <= the Teradata comment limit, so comparing against the raw untruncated
        `desc` would never match for over-length descriptions, re-issuing DDL every run. --#}
    {%- if existing_comment != teradata_truncate_comment(desc | trim, warn=False) -%}
      {%- call statement('alter_column_comment_' ~ loop.index, fetch_result=False) -%}
        comment on column {{ relation }}.{{ rendered_col }} as {{ teradata_escape_comment(desc) }}
      {%- endcall -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro teradata__persist_docs(relation, model, for_relation, for_columns) -%}
  {#-- OTF / Iceberg: native COMMENT ON is unsupported -> skip gracefully. --#}
  {%- if config.get('catalog_name') is not none -%}
    {{ log("persist_docs skipped for OTF relation " ~ relation, info=false) }}
    {{ return('') }}
  {%- endif -%}

  {% if for_relation and config.persist_relation_docs() and model.description %}
    {%- set existing_rel_comment = teradata__get_relation_comment(relation) -%}
    {#-- Compare against the truncated form: what's stored can never exceed the Teradata
        comment limit, so comparing against the raw untruncated description would never
        match for over-length descriptions, re-issuing COMMENT ON DDL on every run. --#}
    {%- if existing_rel_comment != teradata_truncate_comment(model.description | trim, warn=False) -%}
      {% do run_query(teradata__alter_relation_comment(relation, model.description)) %}
    {%- endif -%}
  {% endif %}

  {#-- Functions: Teradata has no COMMENT ON <argument> DDL, only a single
       comment for the whole function, so per-column (per-argument) comments
       cannot be honored. Warn and skip rather than emitting invalid DDL.
       NOTE: as of dbt-core 1.11, functions.yml's `columns:` block is not parsed
       into model.columns (UnparsedFunctionUpdate uses HasColumnProps, not
       HasColumnDocs), so model.columns is always {} here in practice and this
       branch is currently unreachable via YAML — it is intentional future-proofing
       in case a later dbt-core version starts populating model.columns for
       functions. Without this guard, that scenario would silently fall through to
       the elif below and attempt COMMENT ON COLUMN against a function argument,
       which is invalid Teradata DDL and would raise a database error rather than
       a graceful warning. Covered directly in tests/unit/test_persist_docs.py
       (TestPersistDocsFunctionColumnsSkipped) via a synthetic model.columns dict. --#}
  {% if for_columns and config.persist_column_docs() and model.columns and relation.type == 'function' %}
    {{ exceptions.warn("persist_docs 'columns' config is not supported for Teradata functions (arguments cannot carry individual comments); skipping column comments for '" ~ relation ~ "'.") }}
  {% elif for_columns and config.persist_column_docs() and model.columns %}
    {% set existing_comments = teradata__get_column_comments(relation) %}
    {% set existing_column_names = existing_comments.keys() | list %}
    {% set filtered_columns = teradata__validate_doc_columns(relation, model.columns, existing_column_names) %}
    {% do teradata__alter_column_comment(relation, filtered_columns, existing_comments) %}
  {% endif %}
{%- endmacro %}
