{%- materialization view, adapter='teradata' -%}

    -- calling the macro set_query_band() which will set the query_band for this materialization as per the user_configuration
    {% do set_query_band() %}
    {% set relations = materialization_view_default() %}     -- calling thr default view materialization from dbt-core

    {{ return(relations) }}
{%- endmaterialization -%}