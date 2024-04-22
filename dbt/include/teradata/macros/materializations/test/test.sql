{% macro teradata__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    SELECT
      {{ fail_calc }} AS failures,
      CASE
      	WHEN {{ fail_calc }} {{ warn_if | replace("!=","<>") }} THEN 'true'
      	ELSE 'false'
      END AS should_warn,
      CASE
      	WHEN {{ fail_calc }} {{ error_if | replace("!=","<>") }} THEN 'true'
      	ELSE 'false'
      END AS should_error
    FROM (
      {{ main_sql }}
      {{ "SAMPLE " ~ limit if limit != none }}
    ) dbt_internal_test
{%- endmacro %}


{%- materialization test, adapter='teradata' -%}

    -- calling the macro set_query_band() which will set the query_band for this materialization as per the user_configuration
    {% do set_query_band() %}
    {% set relations = materialization_test_default() %}    -- calling the default test materialization from dbt-core

    {{ return(relations) }}
{% endmaterialization %}