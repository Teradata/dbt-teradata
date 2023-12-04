-- Created this macro in adapter, there is no default implementation of this macro in dbt-core
-- This macro will fetch the selected columns from DBC.TablesV
{% macro teradata__get_relation_last_modified(information_schema, relations) -%}
  {%- call statement('last_modified', fetch_result=True) -%}
        select DataBaseName as schema,
               TableName as identifier,
               LastAlterTimeStamp as last_modified,
               {{ current_timestamp() }} as snapshotted_at
        from {{ information_schema_name(schema) }}.tablesV
        where (
          {%- for relation in relations -%}
            (upper(DataBaseName) = upper('{{ relation.schema }}') and
             upper(TableName) = upper('{{ relation.identifier }}')){%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
        )
  {%- endcall -%}

  {{ return(load_result('last_modified')) }}

{% endmacro %}