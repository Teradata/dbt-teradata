
{% macro teradata__get_catalog(information_schema, schemas) -%}
    {%- call statement('catalog', fetch_result=True) -%}
    with tables as (

        select
            null as "table_database",
            DatabaseName as "table_schema",
            TableName as "table_name",
            case when TableKind = 'T' then 'table'
                 when TableKind = 'V' then 'view'
                 else TableKind
            end as "table_type",
            null as "table_owner"

        from DBC.tablesV

        where TableKind in ('T', 'V')

    ),

    columns as (

        select
           null as "table_database",
           DatabaseName as "table_schema",
           TableName as "table_name",
           null as "table_comment",

           ColumnName as "column_name",
           ColumnID as "column_index",
           ColumnType as "column_type",
           CommentString as "column_comment"

        from DBC.ColumnsV

    )

    select
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

    from tables

    join columns on
      tables.table_schema = columns.table_schema
      and tables.table_name = columns.table_name

    where (
    {%- for schema in schemas -%}
      upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    )

    order by column_index
    {%- endcall -%}

    {{ return(load_result('catalog').table) }}

{%- endmacro %}


{% macro teradata__information_schema_name(database) -%}
  DBC
{%- endmacro %}
