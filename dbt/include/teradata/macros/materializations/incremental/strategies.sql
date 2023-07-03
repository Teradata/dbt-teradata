
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


{% macro teradata__get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {%- if unique_key -%}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target_relation }}
            where (
                {% for key in unique_key %}
                    {{ tmp_relation }}.{{ key }} = {{ target_relation }}.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            );
        {% else %}    
            DELETE
            FROM {{ target_relation }}
            WHERE ({{ unique_key }}) IN (
                SELECT ({{ unique_key }})
                FROM {{ tmp_relation }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};

        {% endif %}
        
    {%- endif %}

    INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
       SELECT {{ dest_cols_csv }}
       FROM {{ tmp_relation }}
    ;
{%- endmacro %}


{% macro teradata__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% set error_msg= "Unique key is required for merge incremental strategy, please provide unique key in configuration and try again
        or consider using Append strategy" %}
        {% do exceptions.CompilationError(error_msg) %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched then update set
        {% set final_result = [] %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% set quoted_keys = [] %}
            {% for key in unique_key %}
                {% set quoted_key = adapter.quote(key) %}
                {% set _ = quoted_keys.append(quoted_key) %}
            {% endfor %}
            {% for column_name in update_columns -%}
                {% if column_name not in quoted_keys %}
                    {% set snippet %}
                        {{column_name}}=DBT_INTERNAL_SOURCE.{{ column_name }}
                    {% endset %}
                    {% do final_result.append(snippet) %}
                {% endif %}
            {% endfor %}
            {{ final_result | join(',')}}
        {% else %}
            {% for column_name in update_columns -%}
                {% if column_name != adapter.quote(unique_key) -%}
                    {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
                    {%- if not loop.last %}, {%- endif %}
                {% endif %} 
            {%- endfor %}
        {% endif %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        (
            {% for column in dest_columns -%}
                DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        )

{% endmacro %}