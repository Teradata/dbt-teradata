--Decompose the existing get_catalog() macro in order to minimize redundancy with body of get_catalog_relations(). This introduces some additional macros
{% macro teradata__get_catalog(information_schema, schemas) -%}
    {% set use_qvci = var("use_qvci", "false") | as_bool %}
    {{ log("use_qvci set to : " ~ use_qvci) }}

    {%- call statement('catalog', fetch_result=True) -%}
          with tables as (
              {{ teradata__get_catalog_tables_sql(information_schema) }}
              {{ teradata__get_catalog_schemas_where_clause_sql(schemas) }}
          ),
          columns as (
              {{ teradata__get_catalog_columns_sql(information_schema) }}
              {{ teradata__get_catalog_schemas_where_clause_sql(schemas) }}
          )
          {{ teradata__get_catalog_results_sql() }}
    {%- endcall -%}
    {{ return(load_result('catalog').table) }}
{%- endmacro %}

--A new macro, get_catalog_relations() which accepts a list of relations, rather than a list of schemas.
{% macro teradata__get_catalog_relations(information_schema, relations) -%}
    {% set use_qvci = var("use_qvci", "false") | as_bool %}
    {{ log("use_qvci set to : " ~ use_qvci) }}
    
    {%- call statement('catalog', fetch_result=True) -%}
          with tables as (
              {{ teradata__get_catalog_tables_sql(information_schema) }}
              {{ teradata__get_catalog_relations_where_clause_sql(relations) }}
          ),
          columns as (
              {{ teradata__get_catalog_columns_sql(information_schema) }}
              {{ teradata__get_catalog_relations_where_clause_sql(relations) }}
          )
          {{ teradata__get_catalog_results_sql() }}
    {%- endcall -%}
    {{ return(load_result('catalog').table) }}
{%- endmacro %}

--get_catalog_tables_sql() copied straight from pre-existing get_catalog() everything you would normally fetch from DBC.tablesV
{% macro teradata__get_catalog_tables_sql(information_schema) -%}
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
{%- endmacro %}

--get_catalog_columns_sql() copied straight from pre-existing get_catalog() everything you would normally fetch from DBC.ColumnsJQV and DBC.ColumnsV
{% macro teradata__get_catalog_columns_sql(information_schema) -%}
    {% set use_qvci = var("use_qvci", "false") | as_bool %}
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
{%- endmacro %}

{% macro teradata__get_catalog_results_sql() -%}
    ,
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
{%- endmacro %}

--get_catalog_schemas_where_clause_sql(schemas) copied straight from pre-existing get_catalog(). This uses jinja to loop through the provided schema list and make a big WHERE clause of the form:
--     WHERE info_schema.tables.table_schema IN "schema1" OR info_schema.tables.table_schema IN "schema2" [OR ...]
{% macro teradata__get_catalog_schemas_where_clause_sql(schemas) -%}
    WHERE (
        {%- for schema in schemas -%}
            upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} OR {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

--get_catalog_relations_where_clause_sql(relations) this is likely the only new thing
--        This macro is effectively an evolution of the old get_catalog WHERE clause, except now it has the following control flow.

--       Keep in mind that relation in this instance can be a standalone schema, not necessarily an object with a three-part identifier.

--        for relation provided list of relations
--        1. if relation has an identifier and a schema
--            1. then write WHERE clause to filter on both
--        2. elif relation has a schema
--            1. do what was normally done, filter where info_schema.table.table_schema == relation.schema 
--        3. else throw exception. Houston we do not have a something we can use.
    
--        The result of the above is that dbt can, with "laser-precision" fetch metadata for only the relations it needs, rather than the superset of "all the relations in all the schemas in which dbt has relations".
{% macro teradata__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    upper("table_schema") = upper('{{ relation.schema }}')
                    and upper("table_name") = upper('{{ relation.identifier }}')
                )
            {% elif relation.schema %}
                (
                    upper("table_schema") = upper('{{ relation.schema }}')
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

{% macro teradata__information_schema_name(database) -%}
  DBC
{%- endmacro %}
