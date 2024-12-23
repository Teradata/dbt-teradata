
{% macro teradata__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        SELECT DatabaseName AS schema_name
        FROM {{ information_schema_name(database) }}.DatabasesV
    {%- endcall %}

    {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro teradata__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        DROP {{ relation.type }} /*+ IF EXISTS */ {{ relation }};
    {%- endcall %}
{% endmacro %}

{% macro teradata__truncate_relation(relation) -%}
    {% call statement('truncate_relation') -%}
      DELETE {{ relation }} ALL
    {%- endcall %}
{% endmacro %}

{% macro teradata__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set table_kind = config.get('table_kind', default='') -%}
  {%- set table_option = config.get('table_option', default='') -%}
  {%- set with_statistics = config.get('with_statistics', default=False)| as_bool -%}
  {%- set index = config.get('index', default='') -%}
  {% set contract_config = config.get('contract') %}

  {%- if contract_config.enforced -%}
    {{ sql_header if sql_header is not none }}
    {% call statement('create_table') %}
      CREATE {{ table_kind }} TABLE
      {{ relation.include(database=False) }}
      {% if table_option |length -%}
      , {{ table_option }}
      {%- endif -%}

      {# below macro compares the contract information in schema and sql file of model #}
      {{ get_assert_columns_equivalent(sql) }}

      {# below macro loop through user_provided_columns to create DDL with data types and constraints #}
      {{ get_table_columns_and_constraints() }}

      {%- if with_statistics -%}
      AND STATISTICS
      {%- endif %}
      {% if index |length -%}
        {{ index }}
      {%- endif -%};
    {% endcall %}

    insert into {{ relation }} (
      {{ adapter.dispatch('get_column_names', 'dbt')() }}
    )
    {{ get_select_subquery(sql) }}
    ;
  {% else %}
    {#- Changing the code of this macro as in rows_affected are not getting populated in run_results.json -#}
    {#- Changing it from (create table as) to (create with no data + insert+select) -#}
    {{ sql_header if sql_header is not none }}
    {% call statement('create_table') %}
      CREATE {{ table_kind }} TABLE
      {{ relation.include(database=False) }}
      {% if table_option |length -%}
      , {{ table_option }}
      {% endif -%}
        AS (
            {{ sql }}
        ) WITH NO DATA
      {% if with_statistics -%}
        AND STATISTICS
      {%- endif %}
      {% if index |length -%}
        {{ index }}
      {%- endif -%};
    {% endcall %}

    INSERT INTO {{ relation }}
          {{ sql }}
    ;
  {% endif %}
{% endmacro %}

{% macro teradata__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {# below macro compares the contract information in schema and sql file of model #}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif %}

  {{ sql_header if sql_header is not none }}
  REPLACE VIEW {{ relation.include(database=False) }} AS
    {{ sql }}

{% endmacro %}


{% macro teradata__rename_relation(from_relation, to_relation) -%}
  {#
    Teradata rename fails when the relation already exists, so a 2-step process is needed:
    1. Drop the existing relation
    2. Rename the new relation to the existing relation
  #}
  {% call statement('drop_relation') %}
    DROP {{ to_relation.type }} /*+ IF EXISTS */ {{ to_relation }};
  {% endcall %}
  {% call statement('rename_relation') %}
    RENAME {{ to_relation.type }} {{ from_relation }} TO {{ to_relation }}
  {% endcall %}
{% endmacro %}

{% macro teradata__check_schema_exists(database, schema) -%}
    {# no-op #}
    {# see TeradataAdapter.check_schema_exists() #}
{% endmacro %}

{% macro teradata__get_columns_in_relation(relation) -%}
    {% set use_qvci = var("use_qvci", "false") | as_bool %}
     {{ log("use_qvci set to : " ~ use_qvci) }}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
    SELECT
      ColumnsV.ColumnName AS "column",
      CASE
        WHEN ColumnsV.ColumnType = '++' THEN 'TD_ANYTYPE'
        WHEN ColumnsV.ColumnType = 'A1' THEN 'ARRAY'
        WHEN ColumnsV.ColumnType = 'AN' THEN 'ARRAY'
        WHEN ColumnsV.ColumnType = 'I8' THEN 'BIGINT'
        WHEN ColumnsV.ColumnType = 'BO' THEN 'BINARY LARGE OBJECT'
        WHEN ColumnsV.ColumnType = 'BF' THEN 'BYTE'
        WHEN ColumnsV.ColumnType = 'BV' THEN 'BYTE VARYING'
        WHEN ColumnsV.ColumnType = 'I1' THEN 'BYTEINT'
        WHEN ColumnsV.ColumnType = 'CF' THEN 'CHARACTER'
        WHEN ColumnsV.ColumnType = 'CV' THEN 'CHARACTER'
        WHEN ColumnsV.ColumnType = 'CO' THEN 'CHARACTER LARGE OBJECT'
        WHEN ColumnsV.ColumnType = 'D' THEN 'DECIMAL'
        WHEN ColumnsV.ColumnType = 'DA' THEN 'DATE'
        WHEN ColumnsV.ColumnType = 'F' THEN 'DOUBLE PRECISION'
        WHEN ColumnsV.ColumnType = 'I' THEN 'INTEGER'
        WHEN ColumnsV.ColumnType = 'DY' THEN 'INTERVAL DAY'
        WHEN ColumnsV.ColumnType = 'DH' THEN 'INTERVAL DAY TO HOUR'
        WHEN ColumnsV.ColumnType = 'DM' THEN 'INTERVAL DAY TO MINUTE'
        WHEN ColumnsV.ColumnType = 'DS' THEN 'INTERVAL DAY TO SECOND'
        WHEN ColumnsV.ColumnType = 'HR' THEN 'INTERVAL HOUR'
        WHEN ColumnsV.ColumnType = 'HM' THEN 'INTERVAL HOUR TO MINUTE'
        WHEN ColumnsV.ColumnType = 'HS' THEN 'INTERVAL HOUR TO SECOND'
        WHEN ColumnsV.ColumnType = 'MI' THEN 'INTERVAL MINUTE'
        WHEN ColumnsV.ColumnType = 'MS' THEN 'INTERVAL MINUTE TO SECOND'
        WHEN ColumnsV.ColumnType = 'MO' THEN 'INTERVAL MONTH'
        WHEN ColumnsV.ColumnType = 'SC' THEN 'INTERVAL SECOND'
        WHEN ColumnsV.ColumnType = 'YR' THEN 'INTERVAL YEAR'
        WHEN ColumnsV.ColumnType = 'YM' THEN 'INTERVAL YEAR TO MONTH'
        WHEN ColumnsV.ColumnType = 'N' THEN 'NUMBER'
        WHEN ColumnsV.ColumnType = 'D' THEN 'NUMERIC'
        WHEN ColumnsV.ColumnType = 'PD' THEN 'PERIOD(DATE)'
        WHEN ColumnsV.ColumnType = 'PT' THEN 'PERIOD(TIME(n))'
        WHEN ColumnsV.ColumnType = 'PZ' THEN 'PERIOD(TIME(n) WITH TIME ZONE)'
        WHEN ColumnsV.ColumnType = 'PS' THEN 'PERIOD(TIMESTAMP(n))'
        WHEN ColumnsV.ColumnType = 'PM' THEN 'PERIOD(TIMESTAMP(n) WITH TIME ZONE)'
        WHEN ColumnsV.ColumnType = 'F' THEN 'REAL'
        WHEN ColumnsV.ColumnType = 'I2' THEN 'SMALLINT'
        WHEN ColumnsV.ColumnType = 'AT' THEN 'TIME'
        WHEN ColumnsV.ColumnType = 'TS' THEN 'TIMESTAMP'
        WHEN ColumnsV.ColumnType = 'TZ' THEN 'TIME WITH TIME ZONE'
        WHEN ColumnsV.ColumnType = 'SZ' THEN 'TIMESTAMP WITH TIME ZONE'
        WHEN ColumnsV.ColumnType = 'UT' THEN 'USER‑DEFINED TYPE'
        WHEN ColumnsV.ColumnType = 'XM' THEN 'XML'
        WHEN ColumnsV.ColumnType = 'JN' THEN 'JSON'
        ELSE 'N/A'
      END AS dtype,
      CASE
        WHEN ColumnsV.CharType in (1, 2) THEN ColumnsV.ColumnLength
      END AS char_size,
      ColumnsV.DecimalTotalDigits AS numeric_precision,
      ColumnsV.DecimalFractionalDigits AS numeric_scale,
      NULL AS table_database,
      ColumnsV.DatabaseName AS table_schema,
      ColumnsV.TableName AS table_name,
      CASE WHEN TablesV.TableKind = 'T' THEN 'table'
        WHEN TablesV.TableKind = 'O' THEN 'table'
        WHEN TablesV.TableKind = 'V' THEN 'view'
        ELSE TablesV.TableKind
      END AS table_type,
      ColumnsV.ColumnID AS column_index
    FROM
    {% if use_qvci == True -%}
      {{ information_schema_name(relation.schema) }}.ColumnsJQV AS ColumnsV
    {% else %}
      {{ information_schema_name(relation.schema) }}.ColumnsV
    {%- endif %}
    LEFT OUTER JOIN {{ information_schema_name(relation.schema) }}.TablesV
      ON ColumnsV.DatabaseName = TablesV.DatabaseName
      AND ColumnsV.TableName = TablesV.TableName
    WHERE
      TablesV.TableKind IN ('T', 'V', 'O')
      AND ColumnsV.DatabaseName = '{{ relation.schema }}' (NOT CASESPECIFIC)
      AND ColumnsV.TableName = '{{ relation.identifier }}' (NOT CASESPECIFIC)
    ORDER BY
      ColumnsV.ColumnID
    {% endcall %}

    {% set table = load_result('get_columns_in_relation').table %}

    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro teradata__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    SELECT
      NULL AS "database",
      TableName AS name,
      DatabaseName AS "schema",
      CASE WHEN TableKind = 'T' THEN 'table'
        WHEN TableKind = 'O' THEN 'table'
        WHEN TableKind = 'V' THEN 'view'
        ELSE TableKind
      END AS table_type
    FROM {{ information_schema_name(schema_relation.schema) }}.TablesV
    WHERE DatabaseName = '{{ schema_relation.schema }}' (NOT CASESPECIFIC)
      AND TableKind IN ('T', 'V', 'O')

  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro teradata__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro teradata__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    CREATE DATABASE {{ relation.without_identifier().include(database=False) }}
    -- Teradata expects db sizing params on creation. This macro is probably
    -- useful only for testing. For production scenrios, a properly sized
    -- database (schema) will likely exist before dbt gets called
    AS PERMANENT = 60e6, -- 60MB
        SPOOL = 120e6; -- 120MB
  {%- endcall -%}
{% endmacro %}

{% macro teradata__drop_schema(relation) -%}
  {% if relation.schema -%}
    {{ adapter.verify_database(relation.schema) }}
    {%- call statement('drop_schema_delete_database') -%}
    DELETE DATABASE /*+ IF EXISTS */ {{ relation.without_identifier().include(database=False) }} ALL;
    {%- endcall -%}
    {%- call statement('drop_schema_drop_database') -%}
    DROP DATABASE /*+ IF EXISTS */ {{ relation.without_identifier().include(database=False) }};
    {%- endcall -%}
  {%- endif -%}

{% endmacro %}

{% macro teradata__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        SELECT * FROM (
            {{ select_sql }}
        ) AS __dbt_sbq
        WHERE 0=1
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro teradata__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}

  {% set sql -%}

     alter {{ relation.type }} {{ relation }}

            {% for column in add_columns %}
               add {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
            {% endfor %}{{ ',' if add_columns and remove_columns }}

            {% for column in remove_columns %}
                drop {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}

  {%- endset -%}

  {% do run_query(sql) %}

{% endmacro %}

-- set query_band macro which will be called from every materialization to set the query_band as per user configuration
{% macro set_query_band() %}
  {{ log("Setting query_band") }}
  {% set query_band = config.get('query_band') %}
  {% if query_band %}
    {% set query_band = query_band |replace("{model}",model.name) %}
    {% do run_query("set query_band = '{}' update for session;".format(query_band)) %}
    {% set result = run_query("sel GetQueryBand() as qb;") %}
    {% set out = result.columns['qb'].values() %}
    {{ log("Query Band updated to ['{}']\n".format(out)) }}
    {{ print("\n\t Query Band updated to ['{}']\n".format(out)) }}
  {% endif %}
{% endmacro %}
