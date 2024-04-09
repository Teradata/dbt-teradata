{%- materialization table, adapter='teradata' -%}

    {% do set_query_band() %}
    {% set relations = materialization_table_default() %}
    {{ return(relations) }}

{%- endmaterialization -%}