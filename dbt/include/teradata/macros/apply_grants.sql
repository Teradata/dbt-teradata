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

{%- if relation.type == 'function' -%}
{#-- UDFs carry the EXECUTE FUNCTION access right ('EF') in DBC.AllRightsV, not the
     table DML rights (R/U/I/D). Report it as the 'execute' privilege so it matches
     the `grants: {execute: [...]}` config key when dbt diffs current vs. desired grants. --#}
SEL t.Username as grantee, 'execute' as privilege_type FROM DBC.AllRightsV t
WHERE t.DatabaseName='{{TD_db_name}}' and t.Username <> current_user and t.AccessRight = 'EF' and t.tablename='{{TD_table_name}}';
{%- else -%}
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
{%- endif -%}

{% endmacro %}


{#
  -- Teradata requires the `EXECUTE FUNCTION` privilege keyword for UDFs; plain `EXECUTE`
  -- is for macros / stored procedures. dbt's grant config key for functions is `execute`,
  -- so for function relations we render it as `EXECUTE FUNCTION`. Tables/views are
  -- unaffected and keep dbt's default DCL.
#}
{%- macro teradata__function_privilege(privilege) -%}
    {%- if privilege | lower == 'execute' -%}execute function{%- else -%}{{ privilege }}{%- endif -%}
{%- endmacro -%}


{%- macro teradata__get_grant_sql(relation, privilege, grantees) -%}
    {%- if relation.type == 'function' -%}
        grant {{ teradata__function_privilege(privilege) }} on {{ relation.render() }} to {{ grantees | join(', ') }}
    {%- else -%}
        grant {{ privilege }} on {{ relation.render() }} to {{ grantees | join(', ') }}
    {%- endif -%}
{%- endmacro -%}


{%- macro teradata__get_revoke_sql(relation, privilege, grantees) -%}
    {%- if relation.type == 'function' -%}
        revoke {{ teradata__function_privilege(privilege) }} on {{ relation.render() }} from {{ grantees | join(', ') }}
    {%- else -%}
        revoke {{ privilege }} on {{ relation.render() }} from {{ grantees | join(', ') }}
    {%- endif -%}
{%- endmacro -%}


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