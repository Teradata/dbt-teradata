
{% macro teradata__get_catalog(information_schema, schemas) -%}
    {% set use_qvci = var("use_qvci", "false") | as_bool %}
    {{ log("use_qvci set to : " ~ use_qvci) }}

    {%- call statement('catalog', fetch_result=True) -%}

    WITH tables AS (

        SELECT
            NULL AS table_database,
            DatabaseName AS table_schema,
            TableName AS table_name,
            CASE WHEN TableKind = 'T' THEN 'table'
                WHEN TableKind = 'O' THEN 'view'
                WHEN TableKind = 'V' THEN 'view'
                ELSE TableKind
            END AS table_type,
            NULL AS table_owner

        FROM {{ information_schema_name(schema) }}.tablesV

        WHERE TableKind IN ('T', 'V', 'O')
        AND (
          {%- for schema in schemas -%}
            upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} OR {% endif -%}
          {%- endfor -%}
        )

    ),

    columns AS (

        SELECT
           NULL AS table_database,
           DatabaseName AS table_schema,
           TableName AS table_name,
           NULL AS table_comment,

           ColumnName AS column_name,
           ColumnID AS column_index,
           ColumnType AS column_type,
           CommentString AS column_comment

        {% if use_qvci == True -%}
          FROM {{ information_schema_name(schema) }}.ColumnsJQV
        {% else -%}
          FROM {{ information_schema_name(schema) }}.ColumnsV
        {% endif -%}

        WHERE (
          {%- for schema in schemas -%}
            upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} OR {% endif -%}
          {%- endfor -%}
        )

    ),

    columns_transformed AS (

        SELECT
          table_database,
          table_schema,
          table_name,
          table_comment,
          column_name,
          column_index,
          CASE
            WHEN column_type = '++' THEN 'TD_ANYTYPE'
            WHEN column_type = 'A1' THEN 'ARRAY'
            WHEN column_type = 'AN' THEN 'ARRAY'
            WHEN column_type = 'I8' THEN 'BIGINT'
            WHEN column_type = 'BO' THEN 'BINARY LARGE OBJECT'
            WHEN column_type = 'BF' THEN 'BYTE'
            WHEN column_type = 'BV' THEN 'BYTE VARYING'
            WHEN column_type = 'I1' THEN 'BYTEINT'
            WHEN column_type = 'CF' THEN 'CHARACTER'
            WHEN column_type = 'CV' THEN 'CHARACTER'
            WHEN column_type = 'CO' THEN 'CHARACTER LARGE OBJECT'
            WHEN column_type = 'D' THEN 'DECIMAL'
            WHEN column_type = 'DA' THEN 'DATE'
            WHEN column_type = 'F' THEN 'DOUBLE PRECISION'
            WHEN column_type = 'I' THEN 'INTEGER'
            WHEN column_type = 'DY' THEN 'INTERVAL DAY'
            WHEN column_type = 'DH' THEN 'INTERVAL DAY TO HOUR'
            WHEN column_type = 'DM' THEN 'INTERVAL DAY TO MINUTE'
            WHEN column_type = 'DS' THEN 'INTERVAL DAY TO SECOND'
            WHEN column_type = 'HR' THEN 'INTERVAL HOUR'
            WHEN column_type = 'HM' THEN 'INTERVAL HOUR TO MINUTE'
            WHEN column_type = 'HS' THEN 'INTERVAL HOUR TO SECOND'
            WHEN column_type = 'MI' THEN 'INTERVAL MINUTE'
            WHEN column_type = 'MS' THEN 'INTERVAL MINUTE TO SECOND'
            WHEN column_type = 'MO' THEN 'INTERVAL MONTH'
            WHEN column_type = 'SC' THEN 'INTERVAL SECOND'
            WHEN column_type = 'YR' THEN 'INTERVAL YEAR'
            WHEN column_type = 'YM' THEN 'INTERVAL YEAR TO MONTH'
            WHEN column_type = 'N' THEN 'NUMBER'
            WHEN column_type = 'D' THEN 'NUMERIC'
            WHEN column_type = 'PD' THEN 'PERIOD(DATE)'
            WHEN column_type = 'PT' THEN 'PERIOD(TIME(n))'
            WHEN column_type = 'PZ' THEN 'PERIOD(TIME(n) WITH TIME ZONE)'
            WHEN column_type = 'PS' THEN 'PERIOD(TIMESTAMP(n))'
            WHEN column_type = 'PM' THEN 'PERIOD(TIMESTAMP(n) WITH TIME ZONE)'
            WHEN column_type = 'F' THEN 'REAL'
            WHEN column_type = 'I2' THEN 'SMALLINT'
            WHEN column_type = 'AT' THEN 'TIME'
            WHEN column_type = 'TS' THEN 'TIMESTAMP'
            WHEN column_type = 'TZ' THEN 'TIME WITH TIME ZONE'
            WHEN column_type = 'SZ' THEN 'TIMESTAMP WITH TIME ZONE'
            WHEN column_type = 'UT' THEN 'USER‑DEFINED TYPE'
            WHEN column_type = 'XM' THEN 'XML'
            ELSE 'N/A'
          END AS column_type,
          column_comment

        FROM columns

    ),

    joined AS (

      SELECT
          columns_transformed.table_database,
          columns_transformed.table_schema,
          columns_transformed.table_name,
          tables.table_type,
          columns_transformed.table_comment,
          tables.table_owner,
          columns_transformed.column_name,
          columns_transformed.column_index,
          columns_transformed.column_type,
          columns_transformed.column_comment

      FROM tables

      JOIN columns_transformed ON
        tables.table_schema = columns_transformed.table_schema
        AND tables.table_name = columns_transformed.table_name

    )

    SELECT *
    FROM joined
    ORDER BY table_schema, table_name, column_index

    {%- endcall -%}

    {{ return(load_result('catalog').table) }}

{%- endmacro %}


{% macro teradata__information_schema_name(database) -%}
  DBC
{%- endmacro %}
