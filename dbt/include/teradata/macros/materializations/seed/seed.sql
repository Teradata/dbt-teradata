
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
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here (for tmode=ANSI)
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
