/* The macro had to be overridden for Teradata because original macro uses `column` keyword that is not supported
 nor needed in Teradata's `ALTER TABLE` syntax.
*/
{% macro teradata__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  -- running statements one by one as teradata doesn't allow running multiple DDL commands collectively in ANSI mode
  -- and dbt-teradata supports ANSI mode only
  
  {% call statement('alter_column_type') %}
    alter table {{ relation }} add {{ adapter.quote(tmp_column) }} {{ new_column_type }};
  {% endcall %}

  {% call statement('alter_column_type') %}
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
  {% endcall %}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} drop {{ adapter.quote(column_name) }};
  {% endcall %}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} rename {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }};
  {% endcall %}

{% endmacro %}