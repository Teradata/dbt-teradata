{% macro teradata__copy_grants() %}
    {% set copy_grants = config.get('copy_grants', False) %}
    {{ return(copy_grants) }}
{% endmacro %}

{%- macro teradata__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(True) }}
{%- endmacro -%}


{#
  -- When teradata_grants_extended=true (opt-in), get_show_grant_sql returns three columns:
  --   grantee, grantee_type ('user'|'role'), privilege_type
  -- and UNIONs DBC.AllRightsV (user grants) with DBC.AllRoleRightsV (role grants).
  -- standardize_grants_dict in impl.py then prefixes role grantees with 'role:' so that
  -- the dbt diff engine correctly matches them against 'role:...' entries in the grants config.
  --
  -- Without the flag, the result shape is unchanged (two columns: grantee, privilege_type;
  -- no grantee_type, no role union), preserving backward compatibility. Both the flag-on
  -- and flag-off queries read from DBC.AllRightsV rather than the legacy, truncating
  -- DBC.AllRights table (see the branch below for details).
#}
{% macro teradata__get_show_grant_sql(relation) %}
{% set TD_db_name = relation.schema %}
{% set TD_table_name = relation.identifier %}
{% set grants_extended = var('teradata_grants_extended', False) %}

{%- if relation.type == 'function' -%}

{#-- UDFs carry the EXECUTE FUNCTION access right ('EF') in DBC.AllRightsV, not the
     table DML rights (R/U/I/D). Report it as the 'execute' privilege so it matches
     the `grants: {execute: [...]}` config key when dbt diffs current vs. desired grants. --#}
SEL t.Username as grantee,
    {%- if grants_extended %} 'user' as grantee_type, {%- endif %}
    'execute' as privilege_type
FROM DBC.AllRightsV t
WHERE t.DatabaseName='{{TD_db_name}}'
  AND t.Username <> current_user
  AND t.AccessRight = 'EF'
  AND t.tablename='{{TD_table_name}}'
{%- if grants_extended %}
UNION ALL
SEL r.RoleName as grantee,
    'role' as grantee_type,
    'execute' as privilege_type
FROM DBC.AllRoleRightsV r
WHERE r.DatabaseName='{{TD_db_name}}'
  AND r.AccessRight = 'EF'
  AND r.TableName='{{TD_table_name}}'
{%- endif %}

{%- else -%}

WITH privilege AS (
    SELECT privilege_type, abbreviation FROM (SEL 'select' AS privilege_type, 'R' AS abbreviation) AS "DUAL"
    UNION ALL
    SELECT privilege_type, abbreviation FROM (SEL 'update'  AS privilege_type, 'U' AS abbreviation) AS "DUAL"
    UNION ALL
    SELECT privilege_type, abbreviation FROM (SEL 'insert'  AS privilege_type, 'I' AS abbreviation) AS "DUAL"
    UNION ALL
    SELECT privilege_type, abbreviation FROM (SEL 'delete'  AS privilege_type, 'D' AS abbreviation) AS "DUAL"
)
{#-- DBC.AllRights.DatabaseName is CHAR(30) and silently truncates longer
     database/schema names, so the exact-match predicate below would match
     nothing once TD_db_name exceeds 30 characters. DBC.AllRightsV (the view)
     does not truncate, and is what the function/UDF and role branches above
     already use -- keep this branch consistent with them. --#}
SEL t.Username AS grantee,
    {%- if grants_extended %} 'user' AS grantee_type, {%- endif %}
    p.privilege_type
FROM DBC.AllRightsV t CROSS JOIN privilege p
WHERE t.DatabaseName='{{TD_db_name}}'
  AND t.Username <> current_user
  AND t.AccessRight IN ('R','RF','I','U','D')
  AND p.abbreviation = t.AccessRight
  AND t.tablename='{{TD_table_name}}'
{%- if grants_extended %}
UNION ALL
SEL r.RoleName AS grantee,
    'role' AS grantee_type,
    p.privilege_type
FROM DBC.AllRoleRightsV r CROSS JOIN privilege p
WHERE r.DatabaseName='{{TD_db_name}}'
  AND r.AccessRight IN ('R','RF','I','U','D')
  AND p.abbreviation = r.AccessRight
  AND r.TableName='{{TD_table_name}}'
{%- endif %}

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


{#
  -- Strip 'role:' / 'user:' prefixes from a grantee string, returning just the bare name.
  -- Used when building GRANT SQL — Teradata GRANT ... TO works the same for roles and users.
  -- Prefix detection is case-insensitive ('Role:x' == 'role:x'); the name itself is passed
  -- through untouched, since Teradata identifiers are case-insensitive anyway.
#}
{%- macro teradata__grantee_name(grantee) -%}
    {%- set prefix = grantee[:5] | lower -%}
    {%- if prefix == 'role:' or prefix == 'user:' -%}{{ grantee[5:] }}
    {%- else -%}{{ grantee }}
    {%- endif -%}
{%- endmacro -%}


{#
  -- Format a grantee for a REVOKE statement.
  -- Teradata requires `REVOKE ... FROM ROLE <name>` (ROLE keyword mandatory) to target a role;
  -- plain users need no extra keyword.
#}
{%- macro teradata__grantee_for_revoke(grantee) -%}
    {%- set prefix = grantee[:5] | lower -%}
    {%- if prefix == 'role:' -%}ROLE {{ grantee[5:] }}
    {%- elif prefix == 'user:' -%}{{ grantee[5:] }}
    {%- else -%}{{ grantee }}
    {%- endif -%}
{%- endmacro -%}


{%- macro teradata__get_grant_sql(relation, privilege, grantees) -%}
    {%- set grants_extended = var('teradata_grants_extended', False) -%}
    {%- set priv = teradata__function_privilege(privilege) if relation.type == 'function' else privilege -%}
    {%- if grants_extended -%}
        {%- set sql_grantees = [] -%}
        {%- for g in grantees -%}{%- do sql_grantees.append(teradata__grantee_name(g)) -%}{%- endfor -%}
        grant {{ priv }} on {{ relation.render() }} to {{ sql_grantees | join(', ') }}
    {%- else -%}
        grant {{ priv }} on {{ relation.render() }} to {{ grantees | join(', ') }}
    {%- endif -%}
{%- endmacro -%}


{%- macro teradata__get_revoke_sql(relation, privilege, grantees) -%}
    {%- set grants_extended = var('teradata_grants_extended', False) -%}
    {%- set priv = teradata__function_privilege(privilege) if relation.type == 'function' else privilege -%}
    {%- if grants_extended -%}
        {%- set sql_grantees = [] -%}
        {%- for g in grantees -%}{%- do sql_grantees.append(teradata__grantee_for_revoke(g)) -%}{%- endfor -%}
        revoke {{ priv }} on {{ relation.render() }} from {{ sql_grantees | join(', ') }}
    {%- else -%}
        revoke {{ priv }} on {{ relation.render() }} from {{ grantees | join(', ') }}
    {%- endif -%}
{%- endmacro -%}


{#
  -- When teradata_grants_extended=true, Teradata does not allow mixing `ROLE <name>` and
  -- bare user-names in a single REVOKE ... FROM clause.  Rather than trying to detect which
  -- DCL macro was dispatched (a brittle macro-identity check), we always split role grantees
  -- and user grantees into separate statements while the flag is on: it is required for
  -- REVOKE and harmless for GRANT (Teradata accepts either form of GRANT ... TO list).
#}
{% macro teradata__get_dcl_statement_list(relation, grant_config, get_dcl_macro) %}
    {%- set grants_extended = var('teradata_grants_extended', False) -%}
    {%- set dcl_statements = [] -%}
    {%- for privilege, grantees in grant_config.items() -%}
        {%- if not grantees -%}
            {#-- Empty grantee list: nothing to GRANT or REVOKE for this privilege --#}
        {%- elif grants_extended -%}
            {#-- Split role and user grantees so each statement's clause is homogeneous --#}
            {%- set role_grantees = [] -%}
            {%- set user_grantees = [] -%}
            {%- for g in grantees -%}
                {%- if g[:5] | lower == 'role:' -%}{%- do role_grantees.append(g) -%}
                {%- else -%}{%- do user_grantees.append(g) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if role_grantees -%}
                {%- do dcl_statements.append(get_dcl_macro(relation, privilege, role_grantees)) -%}
            {%- endif -%}
            {%- if user_grantees -%}
                {%- do dcl_statements.append(get_dcl_macro(relation, privilege, user_grantees)) -%}
            {%- endif -%}
        {%- else -%}
            {%- do dcl_statements.append(get_dcl_macro(relation, privilege, grantees)) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(dcl_statements) }}
{% endmacro %}


{#
  -- In extended mode `standardize_grants_dict` qualifies every grantee read back from
  -- Teradata with its type ('role:<name>' / 'user:<name>'), following the same convention
  -- dbt-redshift uses.  The grants config, however, allows bare names as the backward
  -- compatible spelling for users.  Normalize the config the same way — bare names become
  -- 'user:<name>' — so both sides of dbt's diff use one representation and a run that
  -- changes nothing issues no REVOKE/GRANT.
#}
{% macro teradata__apply_grants(relation, grant_config, should_revoke=True) %}
    {%- set grants_extended = var('teradata_grants_extended', False) -%}
    {%- if grants_extended and grant_config -%}
        {%- set normalized_config = {} -%}
        {%- for privilege, grantees in grant_config.items() -%}
            {%- set normalized_grantees = [] -%}
            {%- for grantee in grantees -%}
                {%- set qualified = grantee if grantee[:5] | lower in ['role:', 'user:'] else 'user:' ~ grantee -%}
                {%- if qualified not in normalized_grantees -%}
                    {%- do normalized_grantees.append(qualified) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- do normalized_config.update({privilege: normalized_grantees}) -%}
        {%- endfor -%}
        {%- set grant_config = normalized_config -%}
    {%- endif -%}
    {{ return(default__apply_grants(relation, grant_config, should_revoke)) }}
{% endmacro %}


{% macro teradata__call_dcl_statements(dcl_statement_list) %}
    {#
      -- Teradata does not support running multiple DCL statements as a single statement.
    #}
    {% for dcl_statement in dcl_statement_list %}
        {% call statement('grants') %}
        {{ dcl_statement }};
        {% endcall %}
    {% endfor %}
{% endmacro %}