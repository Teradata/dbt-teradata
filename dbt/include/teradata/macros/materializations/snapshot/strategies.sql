
{% macro teradata__snapshot_hash_arguments(args) -%}
    HASHROW({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar(50)), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}

{% macro snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) -%}
    {%- if not target_exists -%}
        {#-- no table yet -> return whatever the query does --#}
        {{ return((false, query_columns)) }}
    {%- endif -%}

    {#-- handle any schema changes --#}
    {%- set target_relation = adapter.get_relation(database=None, schema=node.schema, identifier=node.alias) -%}

    {% if check_cols_config == 'all' %}
        {%- set query_columns = get_columns_in_query(node['compiled_code']) -%}

    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {#-- query for proper casing/quoting, to support comparison below --#}
        {%- set select_check_cols_from_target -%}
            {#-- N.B. The whitespace below is necessary to avoid edge case issue with comments --#}
            {#-- See: https://github.com/dbt-labs/dbt-core/issues/6781 --#}
            select {{ check_cols_config | join(', ') }} from (
                {{ node['compiled_code'] }}
            ) subq
        {%- endset -%}
        {% set query_columns = get_columns_in_query(select_check_cols_from_target) %}

    {% else %}
        {% do exceptions.raise_compiler_error("Couldn’t validate a target snapshot table. The value specified for 'check_cols_config' is invalid. Correct the model ensuring 'check_cols_config' values map to actual column names in target snapshot table and retry.") %}
    {% endif %}

    {%- set existing_cols = adapter.get_columns_in_relation(target_relation) | map(attribute = 'name') | list -%}
    {%- set ns = namespace() -%} {#-- handle for-loop scoping with a namespace --#}
    {%- set ns.column_added = false -%}

    {%- set intersection = [] -%}
    {%- for col in query_columns -%}
        {%- if col in existing_cols -%}
            {%- do intersection.append(adapter.quote(col)) -%}
        {%- else -%}
            {% set ns.column_added = true %}
        {%- endif -%}
    {%- endfor -%}
    {{ return((ns.column_added, intersection)) }}
{%- endmacro %}

{#-- This macro is copied varbatim from dbt-core. The only delta is that != operator is replaced with <> #}
{% macro snapshot_check_strategy(node, snapshotted_rel, current_rel, model_config, target_exists) %}
    {# The model_config parameter is no longer used, but is passed in anyway for compatibility. #}
    {% set check_cols_config = config.get('check_cols') %}
    {% set primary_key = config.get('unique_key') %}
    {% set hard_deletes = adapter.get_hard_deletes_behavior(config) %}
    {% set invalidate_hard_deletes = hard_deletes == 'invalidate' %}
    {% set updated_at = config.get('updated_at') or snapshot_get_time() %}

    {% set column_added = false %}

    {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) %}

    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        {{ get_true_sql() }}
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

    {% set scd_args = api.Relation.scd_args(primary_key, updated_at) %}
    {% set scd_id_expr = snapshot_hash_arguments(scd_args) %}
    {% set snapshot_hash_udf = config.get('snapshot_hash_udf') %}
    {% if snapshot_hash_udf is not none %}
        {% set scd_id_expr = scd_id_expr |replace("HASHROW",snapshot_hash_udf) %}
    {% endif %}


    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "hard_deletes": hard_deletes
    }) %}
{% endmacro %}
