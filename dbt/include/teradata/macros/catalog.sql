
{% macro teradata__get_catalog(information_schema, schemas) -%}
    {%- call statement('catalog', fetch_result=True) -%}
    WITH tables AS (

        SELECT
            NULL AS "table_database",
            DatabaseName AS "table_schema",
            TableName AS "table_name",
            CASE WHEN TableKind = 'T' THEN 'table'
                 WHEN TableKind = 'V' THEN 'view'
                 ELSE TableKind
            END AS "table_type",
            NULL AS "table_owner"

        FROM {{ information_schema_name(schema) }}.tablesV

        WHERE TableKind IN ('T', 'V')

    ),

    columns AS (

        SELECT
           NULL AS "table_database",
           DatabaseName AS "table_schema",
           TableName AS "table_name",
           NULL AS "table_comment",

           ColumnName AS "column_name",
           ColumnID AS "column_index",
           ColumnType AS "column_type",
           CommentString AS "column_comment"

        FROM {{ information_schema_name(schema) }}.ColumnsV

    )

    SELECT
        columns.table_database,
        columns.table_schema,
        columns.table_name,
        tables.table_type,
        columns.table_comment,
        tables.table_owner,
        columns.column_name,
        columns.column_index,
        columns.column_type,
        columns.column_comment

    FROM tables

    JOIN columns ON
      tables.table_schema = columns.table_schema
      AND tables.table_name = columns.table_name

    WHERE (
    {%- for schema in schemas -%}
      upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} OR {% endif -%}
    {%- endfor -%}
    )

    ORDER BY column_index
    {%- endcall -%}

    {{ return(load_result('catalog').table) }}

{%- endmacro %}


{% macro teradata__information_schema_name(database) -%}
  DBC
{%- endmacro %}
