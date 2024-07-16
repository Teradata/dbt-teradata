
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
    {% call statement('delete_table') %}
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
    {% endcall %}
    {%- endif %}
    INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
       SELECT {{ dest_cols_csv }}
       FROM {{ tmp_relation }}
    ;
{%- endmacro %}


{% macro teradata__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
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
        {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key %}
    when matched then update set
        {% set quoted_keys = [] %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set quoted_key = adapter.quote(key) %}
                {% do quoted_keys.append(quoted_key) %}
            {% endfor %}
        {% else %}
            {% do quoted_keys.append(adapter.quote(unique_key)) %}
        {% endif %}

        {% set final_result = [] %}
        {% for column_name in update_columns -%}
            {% if column_name not in quoted_keys %}
                {% set snippet %}
                    {{column_name}}=DBT_INTERNAL_SOURCE.{{ column_name }}
                {% endset %}
                {% do final_result.append(snippet) %}
            {% endif %}
        {% endfor %}

        {{ final_result | join(',')}}
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

{% macro drop_staging_tables_for_valid_history(table_names) %}
    {% for table_name in table_names %}
        {% call statement('dropping existing staging table ' ~ table_name) %}
            DROP table /*+ IF EXISTS */ {{ table_name }};
        {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro create_staging_tables_for_valid_history(table_names, target) %}
    {% for table_name in table_names %}
        {% call statement('creating staging table ' ~ table_name) %}
            create set table {{ table_name }} as {{ target }} with no data;
        {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro teradata__get_incremental_valid_history_sql(target, source, unique_key, valid_period, valid_from, valid_to, use_valid_to_time, history_column_in_target, resolve_conflicts) -%}
--    {{ log("**************** in teradata__get_incremental_valid_history_sql macro")  }}
--    {{ log("**************** target: " ~ target)  }}
--    {{ log("**************** source: " ~ source)  }}
--    {{ log("**************** unique_key: " ~ unique_key)  }}
--    {{ log("**************** valid_period: " ~ valid_period)  }}
--    {{ log("**************** valid_from: " ~ valid_from)  }}
--    {{ log("**************** valid_to: " ~ valid_to)  }}
--    {{ log("**************** use_valid_to_time: " ~ use_valid_to_time)  }}
--    {{ log("**************** history_column_in_target: " ~ history_column_in_target)  }}
--    {{ log("**************** resolve_conflicts: " ~ resolve_conflicts)  }}
    {%- set exclude_columns = [unique_key , valid_from] -%}

    {%- set source_columns = adapter.get_columns_in_relation(source) -%}
--    {{ log("**************** source_columns: " ~ source_columns)  }}

    {%- set target_columns = adapter.get_columns_in_relation(target) -%}
--    {{ log("**************** target_columns: " ~ target_columns)  }}

    {% set remaining_cols = [] %}
    {% set datatype_of_unique_key = [] %}
    {% for column in source_columns %}
--        {{ log("**************** column: " ~ column)  }}
--        {{ log("**************** column.column: " ~ column.column)  }}
--        {{ log("**************** column.data_type: " ~ column.data_type)  }}
        {% if column.column | lower == unique_key | lower %}
            {%- do datatype_of_unique_key.append(column.data_type) -%}
        {% endif %}
        {% if column.column | lower not in exclude_columns | map("lower") | list %}
             {%- do remaining_cols.append(column) -%}
        {% endif %}
    {% endfor %}
--    {{ log("**************** remaining_cols: " ~ remaining_cols)  }}
--    {{ log("**************** datatype_of_unique_key: " ~ datatype_of_unique_key | join(',')) }}

    {% if unique_key %}
        {% if resolve_conflicts == "yes" %}
            {% if use_valid_to_time == "no" %}
                {% set end_date= "'9999-12-31 23:59:59.999999+00:00'" %}
            {% endif %}

            {% set random_value = range(0,99999) | random %}
            {%- set staging_tables = [
                target.replace_path(identifier='hist_prep_1_' ~ random_value),
                target.replace_path(identifier='hist_prep_2_' ~ random_value),
                target.replace_path(identifier='hist_prep_3_' ~ random_value)
                ]
            -%}
--            {{ log("**************** random_value: " ~ random_value)  }}
--            {{ log("**************** staging_tables: " ~ staging_tables) }}

            {{ drop_staging_tables_for_valid_history(staging_tables) }}
            {{ create_staging_tables_for_valid_history(staging_tables, target) }}
            {% call statement('removing_duplicates') %}
                insert into  {{ staging_tables[0] }}
                        sel distinct
                    {{ unique_key }}
                    ,PERIOD({{ valid_from }}, {{ end_date }} (timestamp))
                    ,
                    {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                    {%- endfor %}
                    from {{ source }}
                    qualify rank() over(partition by {{ unique_key }}, {{ valid_from }} order by
                    {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                    {%- endfor %}
                    )=1;
            {% endcall %}

            {% call statement('adjust overlapping slices') %}
                ins {{ staging_tables[1] }}
                sel
                {{ unique_key }}
                ,PERIOD(
                    begin({{ history_column_in_target }})
                    ,coalesce(lead(begin({{ history_column_in_target }})) over(partition by {{ unique_key }} order by begin({{ history_column_in_target }})),({{ end_date }}(timestamp)))
                 )
                ,
                {% for column in remaining_cols -%}
                    {{ column.column }}
                    {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                from
                (
                    sel * from {{ staging_tables[0] }}
                    union
                    sel * from  {{ target }} t
                    where exists
                    (	sel 1
                        from {{ staging_tables[0] }} s
                        where s.{{ unique_key }}=t.{{ unique_key }}
                        and s.{{ history_column_in_target }} OVERLAPS t.{{ history_column_in_target }}
                    )
                    and not exists
                    (	sel 1
                        from {{ staging_tables[0] }} s
                        where s.{{ unique_key }}=t.{{ unique_key }}
                        and
                        (
                        begin(s.{{ history_column_in_target }})=begin(t.{{ history_column_in_target }})
                        or s.{{ history_column_in_target }} contains t.{{ history_column_in_target }}
                        )
                    )
                ) a;
            {% endcall %}

            {% call statement('compact history') %}
                ins {{ staging_tables[2] }}
                with subtbl as (sel * from {{ staging_tables[1] }})
                sel {{ unique_key }}, {{ history_column_in_target }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                FROM TABLE
                (
                TD_SYSFNLIB.TD_NORMALIZE_MEET
                (
                NEW VARIANT_TYPE(subtbl.{{ unique_key }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                ), subtbl.{{ history_column_in_target }}
                )
                RETURNS ({{ unique_key }} {{ datatype_of_unique_key | join(',') }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }} {{ column.data_type }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                , {{ history_column_in_target }} PERIOD(TIMESTAMP(6)))
                HASH BY {{ unique_key }}
                LOCAL ORDER BY {{ unique_key }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                , {{ history_column_in_target }}
                )
                AS DT({{ unique_key }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                , {{ history_column_in_target }});
            {% endcall %}
            {% call statement('applying changes') %}
                del from  {{ target }} t
                where exists
                (sel 1 from {{ staging_tables[2] }} s where s.{{ unique_key }}=t.{{ unique_key }} and s.{{ history_column_in_target }} overlaps t.{{ history_column_in_target }});
                ins  {{ target }} sel * from {{ staging_tables[2] }};
            {% endcall %}
            {{ drop_staging_tables_for_valid_history(staging_tables) }}
        {% else %}
            {% set error_msg= "Failed" %}
            {% do exceptions.raise_compiler_error(error_msg) %}
        {% endif %}
    {% else %}
        {% set error_msg= "Unique key is required for valid_history incremental strategy, please provide unique key in configuration and try again" %}
        {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}
{% endmacro %}