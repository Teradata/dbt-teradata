{%- materialization table, adapter='teradata' -%}

    -- calling the macro set_query_band() which will set the query_band for this materialization as per the user_configuration
    {% do set_query_band() %}

    {%- set catalog_name = config.get('catalog_name', none) -%}
    {% if catalog_name is not none %}
        {#-- OTF models: create directly at target, skip intermediate/rename pattern.
             DATALAKE tables use 3-part naming that can't be renamed via standard DDL,
             so we cannot use the build-tmp-then-rename pattern from the default
             table materialization. Trade-off: a failed CREATE leaves the target
             dropped (non-atomic re-materialization). --#}
        {%- set grant_config = config.get('grants') -%}
        {%- set existing_relation = load_cached_relation(this) -%}
        {%- set target_relation = this.incorporate(type='table') -%}

        {{ run_hooks(pre_hooks) }}

        {#- Drop existing relation up front. teradata__drop_relation reads the
            current model's catalog_name config, so the drop targets the OTF
            table (not a hypothetical native table at the same name). -#}
        {% if existing_relation is not none %}
            {% do adapter.drop_relation(existing_relation) %}
        {% endif %}

        {{ teradata__create_otf_table_as(target_relation, sql, catalog_name) }}

        {{ run_hooks(post_hooks) }}

        {% do persist_docs(target_relation, model) %}

        {% set should_revoke_grants = should_revoke(existing_relation, full_refresh_mode=True) %}
        {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke_grants) %}

        {% do adapter.commit() %}
        {% do adapter.cache_added(target_relation) %}

        {{ return({'relations': [target_relation]}) }}
    {% else %}
        {% set relations = materialization_table_default() %}   -- calling the default table materialization from dbt-core
        {{ return(relations) }}
    {% endif %}

{%- endmaterialization -%}
