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
        {#- Guard against config combinations that are not supported on the OTF path.
            Teradata-native options do not apply to Iceberg/Delta tables. -#}
        {%- set unsupported = [] -%}
        {%- if config.get('table_kind') -%}{%- do unsupported.append('table_kind') -%}{%- endif -%}
        {%- if config.get('table_option') -%}{%- do unsupported.append('table_option') -%}{%- endif -%}
        {%- if config.get('with_statistics', default=False) | as_bool -%}{%- do unsupported.append('with_statistics') -%}{%- endif -%}
        {%- if config.get('index') -%}{%- do unsupported.append('index') -%}{%- endif -%}
        {%- if unsupported | length > 0 -%}
          {{ exceptions.raise_compiler_error(
              "The following config option(s) are not supported with catalog_name (OTF): "
              ~ unsupported | join(', ')
          ) }}
        {%- endif -%}
        {%- set contract_config = config.get('contract') -%}
        {%- if contract_config is not none and contract_config.enforced -%}
          {{ exceptions.raise_compiler_error(
              "Model contracts (contract.enforced=true) are not yet supported with catalog_name (OTF)."
          ) }}
        {%- endif -%}

        {%- set target_relation = this.incorporate(type='table') -%}

        {{ run_hooks(pre_hooks, inside_transaction=False) }}
        {{ run_hooks(pre_hooks, inside_transaction=True) }}

        {{ teradata__create_otf_table_as(target_relation, sql, catalog_name) }}

        {{ run_hooks(post_hooks, inside_transaction=True) }}

        {% do persist_docs(target_relation, model) %}

        {#- Teradata does not support GRANT on OTF tables (3-part names are
            invalid in GRANT syntax, and OTF objects are not in DBC.AllRights).
            Access control for OTF tables is managed via AUTHORIZATION objects
            and external IAM/OAuth policies. -#}
        {%- if config.get('grants') -%}
          {{ exceptions.warn("grants config is ignored for OTF models — Teradata does not support GRANT on DATALAKE tables.") }}
        {%- endif -%}

        {% do adapter.commit() %}
        {% do adapter.cache_added(target_relation) %}

        {{ run_hooks(post_hooks, inside_transaction=False) }}

        {{ return({'relations': [target_relation]}) }}
    {% else %}
        {% set relations = materialization_table_default() %}   -- calling the default table materialization from dbt-core
        {{ return(relations) }}
    {% endif %}

{%- endmaterialization -%}
