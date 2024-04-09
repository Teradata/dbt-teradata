{%- materialization view, adapter='teradata' -%}

    {% do set_query_band() %}
    {% set relations = materialization_view_default() %}

    {{ return(relations) }}
{%- endmaterialization -%}