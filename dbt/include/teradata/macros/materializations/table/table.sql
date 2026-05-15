{%- materialization table, adapter='teradata' -%}

    -- calling the macro set_query_band() which will set the query_band for this materialization as per the user_configuration
    {% do set_query_band() %}

    {%- set catalog_name = config.get('catalog_name', none) -%}
    {% if catalog_name is not none %}
        {#-- OTF models: create directly at target, skip intermediate/rename pattern.
             DATALAKE tables use 3-part naming that can't be renamed via standard DDL. --#}
        {% set target_relation = this.incorporate(type='table') %}

        {{ run_hooks(pre_hooks) }}
        {{ teradata__create_otf_table_as(target_relation, sql, catalog_name) }}
        {{ run_hooks(post_hooks) }}

        {{ return({'relations': [target_relation]}) }}
    {% else %}
        {% set relations = materialization_table_default() %}   -- calling the default table materialization from dbt-core
        {{ return(relations) }}
    {% endif %}

{%- endmaterialization -%}
