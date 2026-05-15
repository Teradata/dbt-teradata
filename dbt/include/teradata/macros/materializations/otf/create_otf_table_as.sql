{#
  OTF (Open Table Format) table creation macros for Teradata DATALAKE objects.

  These macros generate CREATE TABLE AS statements targeting Iceberg/Delta Lake
  tables via Teradata's native OTF support using 3-part naming:

      <datalake>."<otf_database>"."<table>"

  Supported model config options:
    - catalog_name   : name of the catalog integration (from catalogs.yml)
    - partitioned_by : partition expression, e.g. 'YEAR(dt), country'
    - sorted_by      : sort order, e.g. 'id ASC'
    - tblproperties  : Iceberg table properties, e.g. "'gc.enabled'='true'"
    - purge_mode     : DROP behavior -- 'PURGE ALL' (default) or 'NO PURGE'

  Reference: Teradata "Combined DFES and HLSDS for Java Table Operator for
  Writing Iceberg and Delta Lake Open Table Format" specification, sections
  3.3.5 (CREATE TABLE), 3.3.6 (CREATE TABLE AS), 3.3.8 (DROP TABLE).
#}


{% macro teradata__create_otf_table_as(relation, sql, catalog_name) %}
  {# Route to the appropriate catalog-type-specific macro. #}
  {% set catalog_integration = adapter.get_catalog_integration(catalog_name) %}

  {% if catalog_integration.catalog_type == 'datalake' %}
    {{ teradata__create_datalake_table_as(relation, sql, catalog_integration) }}
  {% else %}
    {{ exceptions.raise_compiler_error(
        "Unsupported catalog_type '" ~ catalog_integration.catalog_type ~ "'."
    ) }}
  {% endif %}

{% endmacro %}


{# ── Native OTF via DATALAKE: 3-part naming ──────────────────────── #}
{#                                                                     #}
{#  datalake_name  → from catalog_integration (catalogs.yml)           #}
{#  otf_database   → from catalog_integration (catalogs.yml)           #}
{#  table name     → relation.identifier (from the dbt model name)     #}
{#  relation.schema is NOT used — it is the Teradata database          #}
{#                                                                     #}
{#  Generated SQL pattern:                                             #}
{#    DROP TABLE /*+ IF EXISTS */ dl."db"."tbl" {PURGE ALL|NO PURGE};  #}
{#    CREATE TABLE dl."db"."tbl"                                       #}
{#      [PARTITIONED BY (...)]                                         #}
{#      [SORTED BY ...]                                                #}
{#      [TBLPROPERTIES(...)]                                           #}
{#      AS (...) WITH DATA;                                            #}

{% macro teradata__create_datalake_table_as(relation, sql, catalog_integration) %}
  {% set datalake_name = catalog_integration.datalake_name %}
  {% set otf_database = catalog_integration.otf_database %}
  {% set otf_relation = datalake_name ~ '."' ~ otf_database ~ '"."' ~ relation.identifier ~ '"' %}

  {# Model-level DDL config options #}
  {% set partitioned_by = config.get('partitioned_by', none) %}
  {% set sorted_by = config.get('sorted_by', none) %}
  {% set tblproperties = config.get('tblproperties', none) %}

  {# purge_mode controls DROP behavior per Teradata DFES spec section 3.3.8:
       'PURGE ALL' (default) — removes catalog entry AND deletes data files
       'NO PURGE'            — removes catalog entry only, keeps data files
     Either clause MUST be specified for OTF DROP TABLE statements. #}
  {% set purge_mode = config.get('purge_mode', 'PURGE ALL') %}
  {% if purge_mode not in ('PURGE ALL', 'NO PURGE') %}
    {{ exceptions.raise_compiler_error(
        "Invalid purge_mode '" ~ purge_mode ~ "'. Must be 'PURGE ALL' or 'NO PURGE'."
    ) }}
  {% endif %}

  {# Drop existing table before re-creation (idempotent via IF EXISTS hint).
     Error 7825 (table not found in external catalog) is suppressed by the
     adapter's add_query() error handler in connections.py. #}
  {% call statement('drop_otf_table', auto_begin=False) %}
    DROP TABLE /*+ IF EXISTS */ {{ otf_relation }} {{ purge_mode }};
  {% endcall %}

  {% call statement('main') %}
    CREATE TABLE {{ otf_relation }}
      {% if partitioned_by %}PARTITIONED BY ({{ partitioned_by }}){% endif %}
      {% if sorted_by %}SORTED BY {{ sorted_by }}{% endif %}
      {% if tblproperties %}TBLPROPERTIES({{ tblproperties }}){% endif %}
      AS ({{ sql }}) WITH DATA;
  {% endcall %}
{% endmacro %}
