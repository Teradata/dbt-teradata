
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
        {% set error_msg= "Couldn’t generate SQL for merge incremental strategy. The merge incremental strategy requires the specification of 'unique_key'. Correct the model and retry." %}
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

{% macro teradata__get_incremental_microbatch_sql(target_relation, tmp_relation, dest_columns,incremental_predicates=None) %}

    {%- set incremental_predicates = [] if incremental_predicates is none else incremental_predicates -%}

    {#-- Add additional incremental_predicates to filter for batch --#}
    {% if model.config.get("__dbt_internal_microbatch_event_time_start") -%}
        {% do incremental_predicates.append("DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " >= TIMESTAMP '" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "'") %}
    {% endif %}
    {% if model.config.__dbt_internal_microbatch_event_time_end -%}
        {% do incremental_predicates.append("DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " < TIMESTAMP '" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "'") %}
    {% endif %}
    

    delete from {{ target_relation }} DBT_INTERNAL_TARGET
    where (
    {% for predicate in incremental_predicates %}
        {%- if not loop.first %}and {% endif -%} {{ predicate }}
    {% endfor %}
    );

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    insert into {{ target_relation }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ tmp_relation }}
    ;
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

{% macro teradata__get_incremental_valid_history_sql(target, source, unique_key, valid_period, use_valid_to_time, resolve_conflicts) -%}
--    {{ log("**************** in teradata__get_incremental_valid_history_sql macro")  }}
--    {{ log("**************** target: " ~ target)  }}
--    {{ log("**************** source: " ~ source)  }}
--    {{ log("**************** unique_key: " ~ unique_key)  }}
--    {{ log("**************** valid_period: " ~ valid_period)  }}
--    {{ log("**************** use_valid_to_time: " ~ use_valid_to_time)  }}
--    {{ log("**************** resolve_conflicts: " ~ resolve_conflicts)  }}
    {% if unique_key is string %}
        {% set unique_key = [unique_key] %}
    {% endif %}

    {%- set exclude_columns = unique_key + [valid_period] -%}

    {%- set source_columns = adapter.get_columns_in_relation(source) -%}
--    {{ log("**************** source_columns: " ~ source_columns)  }}

    {%- set target_columns = adapter.get_columns_in_relation(target) -%}
--    {{ log("**************** target_columns: " ~ target_columns)  }}

    {% set unique_key_cols = [] %}
    {% set remaining_cols = [] %}
    {% set datatype_of_valid_period = [] %}
    {% set join_condition=[] %}

    {% for column in source_columns %}
--        {{ log("**************** column: " ~ column)  }}
--        {{ log("**************** column.column: " ~ column.column)  }}
--        {{ log("**************** column.data_type: " ~ column.data_type)  }}
        {% if column.column | lower in unique_key | map("lower") | list %}
            {%- do unique_key_cols.append(column) -%}
            {%- do join_condition.append('s.' ~ column.column ~ ' = t.' ~ column.column) -%}
        {% endif %}
        {% if column.column | lower not in exclude_columns | map("lower") | list %}
             {%- do remaining_cols.append(column) -%}
        {% endif %}
        {% if column.column | lower == valid_period | lower %}
             {%- do datatype_of_valid_period.append(column.data_type) -%}
        {% endif %}
    {% endfor %}

    {% set datatype_of_valid_period=datatype_of_valid_period[0] %}
    {% set datatype_of_valid_period=datatype_of_valid_period | replace("TIMESTAMP(n)", "TIMESTAMP(6)") %}

    {{ log("**************** datatype_of_valid_period: " ~ datatype_of_valid_period)  }}

    {# Validate the valid_period data type and extract the grain#}
    {% if datatype_of_valid_period[:7] == 'PERIOD(' and datatype_of_valid_period[-1] == ')' %}
        
        {# Extract the data type the PERIOD bounds #}
        {% set datatype_of_valid_datetime = datatype_of_valid_period[7:-1] | trim %}
        
        {{ log("**************** datatype_of_valid_datetime: " ~ datatype_of_valid_datetime)  }}

    {% else %}
        {% do exceptions.raise_compiler_error('Invalid data type: ' ~ datatype_of_valid_period ~ '. Expected a PERIOD() type.') %}
    {% endif %}

    {% set key_column_list = unique_key | join(',') %}
    {% set column_list = key_column_list ~ ',' ~ valid_period ~ ',' ~ remaining_cols | map(attribute='column') | join(',') %}
    {% set join_condition = join_condition | join(' AND ') %}

    {% if unique_key %}
        {% if resolve_conflicts == "yes" %}
            {% if datatype_of_valid_datetime == "DATE" %}
                {% set end_date= "'9999-12-31'" %}
            {% else %}
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
                ({{ column_list }})
                        sel distinct
                    {{ key_column_list }}
                    {%- if use_valid_to_time == "yes" %} 
                    ,{{ valid_period }}
                    {% else -%} 
                    ,PERIOD(begin({{ valid_period }}), cast({{ end_date }} as {{datatype_of_valid_datetime}}))
                    {% endif -%} 
                    {% for column in remaining_cols -%}
                        ,{{ column.column }}
                    {%- endfor %}
                    from {{ source }}
                    qualify rank() over(partition by {{ key_column_list }}, begin({{ valid_period }}) order by
                    {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                    {%- endfor %}
                    )=1;
            {% endcall %}

            {% call statement('adjust overlapping slices') %}
                ins {{ staging_tables[1] }}
                ({{ column_list }})
                sel
                {{ key_column_list }}
                ,PERIOD(
                    begin({{ valid_period }})
                    ,coalesce(lead(begin({{ valid_period }})) over(partition by {{ key_column_list }} order by begin({{ valid_period }})),cast({{ end_date }} as {{datatype_of_valid_datetime}}))
                 )
                {% for column in remaining_cols -%}
                    ,{{ column.column }}
                {%- endfor %}
                from
                (
                    sel {{ column_list }} from {{ staging_tables[0] }}
                    union
                    sel {{ column_list }} from  {{ target }} t
                    where exists
                    (	sel 1
                        from {{ staging_tables[0] }} s
                        where {{ join_condition }}
                        and s.{{ valid_period }} OVERLAPS t.{{ valid_period }}
                    )
                    and not exists
                    (	sel 1
                        from {{ staging_tables[0] }} s
                        where {{ join_condition }}
                        and
                        (
                        begin(s.{{ valid_period }})=begin(t.{{ valid_period }})
                        {{ log("**************** use_valid_to_time: " ~ use_valid_to_time) }}
                        {%- if use_valid_to_time == "yes" -%} 
                            or s.{{ valid_period }} contains begin(t.{{ valid_period }})
                        {% endif -%}
                        )
                    )
                    {% if use_valid_to_time == "yes" %} 
                    union all 
                    sel t.{{ unique_key | join(', t.') }}
                        ,PERIOD(
                            end(s.{{ valid_period }}) 
                            ,end(t.{{ valid_period }})
                        )	--Assume all records are valid until changed, we will adjust the Valid To later.
                        {% for column in remaining_cols -%}
                            ,t.{{ column.column }}
                        {%- endfor %}
                
                    from  {{ target }} t
                    join {{ staging_tables[0] }} s 
                        on {{ join_condition }}
                        and  s.{{ valid_period }} overlaps t.{{ valid_period }}
                        and end(s.{{ valid_period }}) < end(t.{{ valid_period }})
                    {% endif -%}                    
                ) a;
            {% endcall %}

            {% call statement('compact history') %}
                ins {{ staging_tables[2] }}
                ({{ column_list }})
                with subtbl as (sel * from {{ staging_tables[1] }})
                sel {{ column_list }}
                FROM TABLE
                (
                TD_SYSFNLIB.TD_NORMALIZE_MEET
                (
                NEW VARIANT_TYPE( subtbl.{{ unique_key | join(', subtbl.') }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                ), subtbl.{{ valid_period }}
                )
                RETURNS (
                {% for column in unique_key_cols+remaining_cols -%}
                        {{ column.column }} {{ column.data_type }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                , {{ valid_period }} {{datatype_of_valid_period}})
                HASH BY {{ key_column_list }}
                LOCAL ORDER BY {{ key_column_list }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                , {{ valid_period }}
                )
                AS DT({{ key_column_list }}
                ,
                {% for column in remaining_cols -%}
                        {{ column.column }}
                        {%- if not loop.last %}, {% endif %}
                {%- endfor %}
                , {{ valid_period }});
            {% endcall %}
            {% call statement('applying changes') %}
                del from  {{ target }} t
                where exists
                (sel 1 from {{ staging_tables[2] }} s where {{ join_condition }} and s.{{ valid_period }} overlaps t.{{ valid_period }});
                ins  {{ target }} ({{ column_list }}) sel {{ column_list }} from {{ staging_tables[2] }};
            {% endcall %}
            {{ drop_staging_tables_for_valid_history(staging_tables) }}
        {% else %}
            {% set error_msg= "The setting resolve_conflicts should be set to yes" %}
            {% do exceptions.raise_compiler_error(error_msg) %}
        {% endif %}
    {% else %}
        {% set error_msg= "Couldn’t generate SQL for valid_history incremental strategy. The valid_history incremental strategy requires the specification of 'unique_key'. Correct the model and retry." %}
        {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}
{% endmacro %}