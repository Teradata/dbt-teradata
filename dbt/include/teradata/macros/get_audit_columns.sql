{% macro get_audit_columns() %}
    cast('{{ invocation_id }}' as varchar(36)) as job_id,
    {{ teradata__snapshot_string_as_time(run_started_at) }} as job_start_tstmp,
    cast(null as timestamp(6) with time zone) as job_end_tstmp
{% endmacro %}


{% macro audit_columns_post_hook() %}
    {% if execute %}
        {% set materialized = config.get('materialized') %}
        {% set catalog_name = config.get('catalog_name') %}
        {% set columns = [] %}
        {% if materialized in ['table', 'incremental'] and not catalog_name %}
            {% set columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list %}
        {% endif %}
          {% if materialized in ['table', 'incremental'] and not catalog_name
              and 'job_id' in columns
              and 'job_start_tstmp' in columns
              and 'job_end_tstmp' in columns %}
            update {{ this }}
            set job_end_tstmp = current_timestamp(6) at time zone 'GMT'
            where job_id = '{{ invocation_id }}'
              and job_end_tstmp is null
        {% else %}
            {{ log(
                "audit_columns_post_hook skipped for unsupported materialization "
                ~ materialized ~ " (catalog_name=" ~ catalog_name
                ~ ", audit_columns_present="
                ~ ('job_id' in columns and 'job_start_tstmp' in columns and 'job_end_tstmp' in columns)
                ~ ")",
                info=true
            ) }}
        {% endif %}
    {% endif %}
{% endmacro %}
