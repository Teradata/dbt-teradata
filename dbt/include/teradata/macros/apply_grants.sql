{% macro teradata__copy_grants() %}
    {% set copy_grants = config.get('copy_grants', False) %}
    {{ return(copy_grants) }}
{% endmacro %}

{%- macro teradata__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(True) }}
{%- endmacro -%}


{% macro teradata__get_show_grant_sql(relation) %}
{% set TD_db_name= relation.schema %}
{% set TD_table_name=relation.identifier %}

with privilege as(
SELECT privilege_type, abbreviation FROM (sel 'select' as privilege_type, 'R' as abbreviation) As "DUAL"
UNION ALL
SELECT privilege_type, abbreviation FROM (sel 'update' as privilege_type, 'U' as abbreviation) As "DUAL"
UNION ALL
SELECT privilege_type, abbreviation FROM (sel 'insert' as privilege_type, 'I' as abbreviation) As "DUAL"
UNION ALL
SELECT privilege_type, abbreviation FROM (sel 'delete' as privilege_type, 'D' as abbreviation) As "DUAL"
)

SEL t.Username as grantee , p.privilege_type FROM DBC.AllRights t cross join privilege p
WHERE t.DatabaseName='{{TD_db_name}}' and t.Username <> current_user and t.AccessRight IN ('R','RF','I','U','D') and p.abbreviation=t.AccessRight and t.tablename='{{TD_table_name}}';

{% endmacro %}


{% macro teradata__call_dcl_statements(dcl_statement_list) %}
    {#
      -- We have overridden this macro as teradata doesn't support running multiple dcl statement as a single statement
    #}
        {% for dcl_statement in dcl_statement_list %}
            {% call statement('grants') %}
            {{ dcl_statement }};
            {% endcall %}
        {% endfor %}
    
{% endmacro %}