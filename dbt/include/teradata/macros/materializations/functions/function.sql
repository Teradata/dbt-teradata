{#
  Teradata SQL scalar UDF support for the dbt 1.11 `function` resource type.

  Supported:  type: scalar, language: sql
  Unsupported: type: aggregate — raises compile error via guard macro.

  Note: dbt 1.11 ignores the `language:` key in functions.yml entirely;
  FunctionNode.language always defaults to 'sql'. The supported_languages
  declaration in the materialization override is therefore a no-op guard
  for future-proofing, not an active filter today.

  The model body (.sql file) must be a bare Teradata RETURN statement, e.g.:
      RETURN a + b;

  Teradata emits:
      REPLACE FUNCTION <schema>.<name> (<args>)
      RETURNS <data_type>
      LANGUAGE SQL
      CONTAINS SQL                 -- only valid data access for LANGUAGE SQL / INLINE TYPE 1
      [DETERMINISTIC|NOT DETERMINISTIC]  -- config: volatility (omitted if not set)
      COLLATION INVOKER
      INLINE TYPE 1
      RETURN ...;

  persist_docs: the function-level `description:` is written via `COMMENT ON
  FUNCTION ... AS '...'` (see teradata__persist_docs / teradata__alter_relation_comment
  in persist_docs.sql). Column-level persist_docs (`columns: true`) is skipped with a
  warning — Teradata has no per-argument comment DDL, only a single comment for the
  whole function.
#}

{# Declare only SQL as supported. dbt 1.11 ignores `language:` in functions.yml
   so this is currently future-proofing, but keeps the door closed for any
   future dbt version that does parse the language field. #}
{% materialization function, adapter='teradata', supported_languages=['sql'] %}
    {{ return(materialization_function_default()) }}
{% endmaterialization %}


{# ---- scalar UDF DDL ---- #}
{% macro teradata__scalar_function_sql(target_relation) %}
    REPLACE FUNCTION {{ target_relation.render() }} ({{ teradata__formatted_function_args() }})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE SQL
    {{ teradata__function_sql_data_access() }}
    {{ teradata__function_volatility_sql() }}
    COLLATION INVOKER
    INLINE TYPE 1
    {{ model.compiled_code }}
{% endmacro %}


{# ---- argument list: "name TYPE, name TYPE, ..." ---- #}
{% macro teradata__formatted_function_args() %}
    {%- set args = [] -%}
    {%- for arg in model.arguments -%}
        {%- do args.append(arg.name ~ ' ' ~ arg.data_type) -%}
    {%- endfor -%}
    {{ args | join(', ') }}
{% endmacro %}


{# ---- data access clause ----
   Teradata SQL UDFs with LANGUAGE SQL / INLINE TYPE 1 only accept CONTAINS SQL.
   NO SQL, READS SQL DATA, and MODIFIES SQL DATA are for external (C/Java) UDFs
   and stored procedures — Teradata raises Error 3706 if they are used here. #}
{% macro teradata__function_sql_data_access() %}
    CONTAINS SQL
{% endmacro %}


{# ---- volatility: maps dbt vocabulary to Teradata DETERMINISTIC / NOT DETERMINISTIC ---- #}
{% macro teradata__function_volatility_sql() %}
    {%- set volatility = model.config.get('volatility') -%}
    {%- if volatility == 'deterministic' -%}
        DETERMINISTIC
    {%- elif volatility in ('stable', 'non-deterministic') -%}
        NOT DETERMINISTIC
    {%- elif volatility is not none -%}
        {{ exceptions.warn(
            "Unsupported volatility '" ~ volatility ~ "' on function '" ~ model.name ~ "' — ignoring."
        ) }}
    {%- endif -%}
{% endmacro %}


{# ---- execution: run DDL, apply grants, persist docs, commit ---- #}
{% macro teradata__function_execute_build_sql(build_sql, existing_relation, target_relation) %}
    {% do set_query_band() %}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {# Grants on a function relation are rendered as EXECUTE FUNCTION DCL — see
       teradata__get_grant_sql / teradata__get_revoke_sql in apply_grants.sql. #}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {# persist_docs: teradata__persist_docs (persist_docs.sql) handles function relations
       by emitting `COMMENT ON FUNCTION ... AS '...'` for the relation-level description.
       Column-level persist_docs (`columns: true`) is skipped there with a warning, since
       Teradata has no per-argument comment DDL. #}
    {% do persist_docs(target_relation, model) %}

    {% do adapter.commit() %}
{% endmacro %}


{# ---- aggregate UDF guard: raise a clear error rather than emitting invalid DDL ----
   dbt dispatches on f"{config.type}_function_{config.language}" (see
   BaseRelation.get_function_macro_name in dbt-adapters), so for `type: aggregate` /
   `language: sql` the dispatched macro name is `aggregate_function_sql` — NOT
   `get_aggregate_function_create_replace_signature` (that's an internal helper the
   base `default__aggregate_function_sql` composition would call, but no such
   top-level `default__aggregate_function_sql` macro exists in dbt-adapters today,
   so without this override the failure is an opaque "no macro found" dispatch
   error rather than a clear message). #}
{% macro teradata__aggregate_function_sql(target_relation) %}
    {{ exceptions.raise_compiler_error(
        "Aggregate user-defined functions (type: aggregate) are not supported by dbt-teradata. "
        ~ "Use type: scalar for SQL scalar UDFs."
    ) }}
{% endmacro %}
