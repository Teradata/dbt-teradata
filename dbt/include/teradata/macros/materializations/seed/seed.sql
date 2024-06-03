
{% macro teradata__get_batch_size() %}
    {{ return(2536) }}
{% endmacro %}

{% macro teradata__load_csv_rows(model, agate_table) %}
    {% set batch_size = get_batch_size() %}
    {% set use_fastload = config.get('use_fastload', default=False) | as_bool %}
    {% if use_fastload %}
        -- Disable chunking when using fastload
        {% set batch_size = agate_table.rows | length %}
    {% else %}
        {% set max_batch_size = teradata__get_batch_size() %}
        {% set batch_size = [batch_size, max_batch_size]|min %}
    {% endif %}

    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.append(row.values()) %}
        {% endfor %}}
        {% set sql %}
            {%- if use_fastload -%}
                {fn teradata_try_fastload}
            {%- endif -%}
                insert into {{ this.render() }} ({{ cols_sql }}) values
                ({%- for column in agate_table.column_names -%}
                    ?
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
        {% endset %}
        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% macro default__get_binding_char() %}
  {{ return('?') }}
{% endmacro %}


{% materialization seed, adapter='teradata' %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  -- calling set_query_band() macro to set the query_band as per the user configuration in yml file
  {% do set_query_band() %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN TRANSACTION` happens here (for tmode=TERA):
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation)) }}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}

  {% set code = 'CREATE' if full_refresh_mode else 'INSERT' %}
  {% set rows_affected = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    {{ create_table_sql }};
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {% set target_relation = this.incorporate(type='table') %}

  -- Apply grants
  {% set grant_config = config.get('grants') %}
  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here (for tmode=ANSI)
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro teradata__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {{ log("model : " ~ model) }}
  {%- set sql_header = model['config'].get('sql_header', None) -%}
  {%- set table_kind = model['config'].get('table_kind', '') -%}
  {%- set table_option = config.get('table_option', '') -%}
  {%- set index = config.get('index', default='') -%}


  {% set sql %}
    {{ sql_header if sql_header is not none }}
    CREATE {{ table_kind }} TABLE {{ this.render() }}
    {% if table_option |length -%}
    , {{ table_option }}
    {%- endif %}
    (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
    {% if index |length -%}
    {{ index }}
    {%- endif -%};

  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}
