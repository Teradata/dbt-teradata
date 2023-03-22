
{% macro teradata__get_incremental_default_sql(arg_dict) %}

    {% do return(get_incremental_append_sql(arg_dict)) %}

{% endmacro %}


{% macro teradata__get_incremental_append_sql(target_relation, tmp_relation,dest_columns) %}

  {% do return(get_insert_into_sql(target_relation, tmp_relation,  dest_columns)) %}

{% endmacro %}


{% macro get_insert_into_sql(target_relation, tmp_relation, dest_columns) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ tmp_relation }}
    

{% endmacro %}