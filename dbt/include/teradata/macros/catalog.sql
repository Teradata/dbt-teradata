{% macro teradata_get_current_timestamp() %}
    {%- call statement('current_timestamp', fetch_result=True) -%}
        select cast(current_timestamp(0) as timestamp(0));
    {% endcall %}
    {{ return(load_result('current_timestamp').table.columns['Current TimeStamp(0)'].values()[0] | trim) }}
{% endmacro %}

{% macro teradata__get_views_from_relations(relations) -%}
    {% set view_relations = [] %}
    {%- for relation in relations -%}
        {% if relation.schema and relation.identifier %}
            {% call statement('check_table_or_view', fetch_result=True) %}
                SELECT TableKind FROM DBC.TablesV WHERE DatabaseName = '{{ relation.schema }}' AND TableName = '{{ relation.identifier }}'
            {% endcall %}
            {% set table_kind = load_result('check_table_or_view').table.columns['TableKind'].values()[0] | trim  %}
            {%- if table_kind == 'V' -%}
                {% do view_relations.append(relation) %}
            {%- endif -%}
        {% endif %}
    {%- endfor %}
    {{ return(view_relations) }}
{%- endmacro %}

{% macro teradata__create_tmp_tables_of_views(view_relations) -%}

    {% set fallback_schema = var("fallback_schema", null) %}
    {{ log("fallback_schema set to : " ~ fallback_schema) }}

    {% set view_tmp_tables_mapping = {} %}
    {%- for relation in view_relations -%}
        {% set timestamp = teradata_get_current_timestamp() %}
        {% set rand = range(1, 100000) | random %}
        {% set uuid = timestamp.replace(":", "").replace("-", "").replace(" ","").replace("+","").replace(".","") ~ rand %}
        {% set temp_relation_for_view = relation.identifier ~ '_tmp_viw_tbl_' ~ uuid %}
        {% set view_tmp_tables_mapping = view_tmp_tables_mapping.update({relation: temp_relation_for_view}) %}
    {%- endfor %}


    {%- for relation, temp_relation_for_view in view_tmp_tables_mapping.items() %}
        {% if fallback_schema==null %}
          {% set schema_name = relation.schema %}
        {% else %}
          {% set schema_name = fallback_schema %}
        {% endif %}

        {{ teradata__drop_tmp_tables_of_views(schema_name, temp_relation_for_view) }}

        {% call statement('creating_table_from_view', fetch_result=False) %}
            CREATE TABLE "{{ schema_name }}"."{{ temp_relation_for_view }}" AS (SELECT * FROM "{{ relation.schema }}"."{{ relation.identifier }}") WITH NO DATA;
        {% endcall %}
        load_result('creating_table_from_view')
    {%- endfor %}
    {{ return(view_tmp_tables_mapping) }}
{%- endmacro %}

{% macro teradata__drop_tmp_tables_of_views(schema_name, temp_relation_for_view) -%}
    {% call statement('drop_existing_table', fetch_result=False) %}
        DROP table /*+ IF EXISTS */ "{{ schema_name }}"."{{ temp_relation_for_view }}";
    {% endcall %}
    load_result('drop_existing_table')
{%- endmacro %}


--Decompose the existing get_catalog() macro in order to minimize redundancy with body of get_catalog_relations(). This introduces some additional macros
{% macro teradata__get_catalog(information_schema, schemas) -%}
    {% set use_qvci = var("use_qvci", False) | as_bool %}
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
          {{ teradata__get_catalog_results_sql(none) }}
    {%- endcall -%}
    {{ return(load_result('catalog').table) }}
{%- endmacro %}

--A new macro, get_catalog_relations() which accepts a list of relations, rather than a list of schemas.
{% macro teradata__get_catalog_relations(information_schema, relations) -%}
    {% set use_qvci = var("use_qvci", False) | as_bool %}
    {{ log("use_qvci set to : " ~ use_qvci) }}
    {% set view_tmp_tables_mapping = {} %}
    {% if use_qvci == False -%}
        {% set view_relations = teradata__get_views_from_relations(relations) %}
        {% set view_tmp_tables_mapping = teradata__create_tmp_tables_of_views(view_relations) %}
    {%- endif %}

    {%- call statement('catalog', fetch_result=True) -%}
          with tables as (
              {{ teradata__get_catalog_tables_sql(information_schema) }}
              {% if use_qvci == False -%}
                {{ teradata__get_catalog_relations_where_clause_sql(relations, view_tmp_tables_mapping) }}
                {% else %}
                    {{ teradata__get_catalog_relations_where_clause_sql(relations, none) }}
                {% endif %}
          ),
          columns as (
              {{ teradata__get_catalog_columns_sql(information_schema) }}
                {% if use_qvci == False -%}
                    {{ teradata__get_catalog_relations_where_clause_sql(relations, view_tmp_tables_mapping) }}
                {% else %}
                    {{ teradata__get_catalog_relations_where_clause_sql(relations, none) }}
                {% endif %}
          )
          {{ teradata__get_catalog_results_sql(view_tmp_tables_mapping) }}
    {%- endcall -%}

    {% set catalog_table = load_result('catalog').table %}

    {% set fallback_schema = var("fallback_schema", null) %}
     {%- for relation, temp_relation_for_view in view_tmp_tables_mapping.items() %}
        {% if fallback_schema==null %}
          {% set schema_name = relation.schema %}
        {% else %}
          {% set schema_name = fallback_schema %}
        {% endif %}
        {{ teradata__drop_tmp_tables_of_views(schema_name, temp_relation_for_view) }}
     {%- endfor %}

    {{ return(catalog_table) }}
{%- endmacro %}

--get_catalog_tables_sql() copied straight from pre-existing get_catalog() everything you would normally fetch from DBC.tablesV
{% macro teradata__get_catalog_tables_sql(information_schema) -%}
    SELECT			
        NULL AS table_database,
        DatabaseName AS table_schema,
        TableName AS table_name,
        CASE WHEN TableKind = 'T' THEN 'table'
            WHEN TableKind = 'O' THEN 'table'
            WHEN TableKind = 'V' THEN 'view'
            ELSE TableKind
        END AS table_type,
        NULL AS table_owner
    FROM {{ information_schema_name(schema) }}.tablesV
{%- endmacro %}

--get_catalog_columns_sql() copied straight from pre-existing get_catalog() everything you would normally fetch from DBC.ColumnsJQV and DBC.ColumnsV
{% macro teradata__get_catalog_columns_sql(information_schema) -%}
    {% set use_qvci = var("use_qvci", False) | as_bool %}
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

{% macro teradata__get_catalog_results_sql(view_tmp_tables_mapping) -%}
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
          {% if view_tmp_tables_mapping is not none and view_tmp_tables_mapping|length > 0 %}
            CASE
              {% for relation, temp_table in view_tmp_tables_mapping.items() %}
                WHEN columns_transformed.table_schema = upper('{{ var("fallback_schema", null) }}') THEN '{{ relation.schema }}'
              {% endfor %}
              ELSE columns_transformed.table_schema
            END as table_schema,
            CASE
            {% for relation, temp_table in view_tmp_tables_mapping.items() %}
                WHEN columns_transformed.table_name = '{{ temp_table }}' THEN '{{ relation.identifier }}'
            {% endfor %}
                ELSE columns_transformed.table_name
            END AS table_name,
          {% else %}
            columns_transformed.table_schema,
            columns_transformed.table_name,
          {% endif %}
          {% if view_tmp_tables_mapping is not none and view_tmp_tables_mapping|length > 0 %}
            CASE
            {% for relation, temp_table in view_tmp_tables_mapping.items() %}
                WHEN columns_transformed.table_name = '{{ temp_table }}' THEN 'view'
            {% endfor %}
                ELSE tables.table_type
            END AS table_type,
          {% else %}
            tables.table_type,
          {% endif %}
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
{% macro teradata__get_catalog_relations_where_clause_sql(relations, view_tmp_tables_mapping) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    {% if view_tmp_tables_mapping is not none and view_tmp_tables_mapping|length > 0 %}

                        {% set temp_table = view_tmp_tables_mapping[relation] %}
                        {% if not temp_table %}
                            upper("table_schema") = upper('{{ relation.schema }}')
                            and upper("table_name") = upper('{{ relation.identifier }}')
                        {% else %}
                            {% if var("fallback_schema", null) != null %}
                                upper("table_schema") = upper('{{ var("fallback_schema", null) }}')
                                and upper("table_name") = upper('{{ temp_table }}')
                            {% else %}
                                upper("table_schema") = upper('{{ relation.schema }}')
                                and upper("table_name") = upper('{{ temp_table }}')
                            {% endif %}
                        {% endif %}
                    {% else %}
                        upper("table_schema") = upper('{{ relation.schema }}')
                        and upper("table_name") = upper('{{ relation.identifier }}')
                    {% endif %}
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
