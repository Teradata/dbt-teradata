{#
  OTF (Open Table Format) table creation macros for Teradata DATALAKE objects.

  These macros generate CREATE TABLE AS statements targeting Iceberg/Delta Lake
  tables via Teradata's native OTF support using 3-part naming:

      "<datalake>"."<otf_database>"."<table>"

  Supported model config options:
    - catalog_name   : name of the catalog integration (from catalogs.yml)
    - partitioned_by : partition expression, e.g. 'YEAR(dt), country'
    - sorted_by      : sort order, e.g. 'id ASC'
    - tblproperties  : Iceberg table properties, e.g. "'gc.enabled'='true'"
    - purge_mode     : DROP behavior -- 'NO PURGE' (default, safe) or 'PURGE ALL'
#}


{% macro teradata__validate_purge_mode(value) %}
  {#- Normalise to upper-case and validate. Returns the validated value so
      callers can use the normalised form directly. -#}
  {%- set normalized = (value or 'NO PURGE') | trim | upper -%}
  {%- if normalized not in ('PURGE ALL', 'NO PURGE') -%}
    {{ exceptions.raise_compiler_error(
        "Invalid purge_mode '" ~ value ~ "'. Must be 'PURGE ALL' or 'NO PURGE'."
    ) }}
  {%- endif -%}
  {{ return(normalized) }}
{% endmacro %}


{% macro teradata__quote_otf_part(part) %}
  {#- Quote a single OTF name part, doubling any embedded quote character.
      This is the single quoting primitive for External OTF identifiers and
      intentionally mirrors TeradataRelation.render() in relation.py, so that
      OTF names produced by DDL/DML macros and by relation rendering are always
      identical (including names that contain a double-quote). -#}
  {%- set qc = '"' -%}
  {{ return(qc ~ (part | string | replace(qc, qc ~ qc)) ~ qc) }}
{% endmacro %}


{% macro teradata__build_otf_relation_name(catalog_integration, identifier) %}
  {#- Canonical builder for the 3-part External OTF relation string:
        "<datalake>"."<otf_database>"."<identifier>"
      This is the single source of truth reused by the OTF create, drop, and
      incremental macros. Quoting is delegated to teradata__quote_otf_part so it
      stays consistent with TeradataRelation.render(). -#}
  {%- set datalake = teradata__quote_otf_part(catalog_integration.datalake_name) -%}
  {%- set otf_database = teradata__quote_otf_part(catalog_integration.otf_database) -%}
  {%- set table_name = teradata__quote_otf_part(identifier) -%}
  {{ return(datalake ~ '.' ~ otf_database ~ '.' ~ table_name) }}
{% endmacro %}


{% macro teradata__drop_otf_table(catalog_integration, identifier, purge_mode) %}
  {#- Drop an OTF table. The purge clause is mandatory for OTF DROP TABLE;
      the validator normalises and rejects unsupported values. -#}
  {%- set validated_purge_mode = teradata__validate_purge_mode(purge_mode) -%}
  {%- set otf_relation = teradata__build_otf_relation_name(catalog_integration, identifier) -%}
  {% call statement('drop_otf_table', auto_begin=False) -%}
    {#- /*+ IF EXISTS */ is not native OTF DDL syntax — it is a sentinel token
        recognised by add_query() in connections.py to suppress error 7825
        (table not found in external catalog) for nonexistent OTF tables. -#}
    DROP TABLE /*+ IF EXISTS */ {{ otf_relation }} {{ validated_purge_mode }};
  {%- endcall %}
{% endmacro %}


{% macro teradata__create_otf_table_as(relation, sql, catalog_name) %}
  {#- Router: dispatch to the catalog-type-specific create macro. -#}
  {% set catalog_integration = adapter.get_catalog_integration(catalog_name) %}

  {% if catalog_integration.catalog_type == 'datalake' %}
    {{ teradata__create_datalake_table_as(relation, sql, catalog_integration) }}
  {% else %}
    {{ exceptions.raise_compiler_error(
        "Unsupported catalog_type '" ~ catalog_integration.catalog_type ~ "'."
    ) }}
  {% endif %}

{% endmacro %}


{#  Native OTF via DATALAKE: 3-part naming
    -----------------------------------------------------------------
    datalake_name  -> from catalog_integration (catalogs.yml)
    otf_database   -> from catalog_integration (catalogs.yml)
    table name     -> relation.identifier (the dbt model name)
    relation.schema is NOT used -- it is the Teradata database, not the OTF database

    Generated SQL pattern:
      DROP TABLE /*+ IF EXISTS */ "dl"."db"."tbl" {NO PURGE|PURGE ALL};
      CREATE TABLE "dl"."db"."tbl"
        [PARTITIONED BY (...)]
        [SORTED BY ...]
        [TBLPROPERTIES(...)]
        AS (...) WITH DATA;
#}

{% macro teradata__create_datalake_table_as(relation, sql, catalog_integration) %}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set otf_relation = teradata__build_otf_relation_name(catalog_integration, relation.identifier) -%}

  {# Model-level DDL config options #}
  {%- set partitioned_by = config.get('partitioned_by', none) -%}
  {%- set sorted_by = config.get('sorted_by', none) -%}
  {%- set tblproperties = config.get('tblproperties', none) -%}
  {%- set purge_mode = config.get('purge_mode') -%}

  {# Drop existing table before re-creation (idempotent via IF EXISTS hint).
     Error 7825 (table not found in external catalog) is suppressed by the
     adapter's add_query() error handler in connections.py. Trade-off: this
     is non-atomic -- if the subsequent CREATE fails, the table is gone. #}
  {{ teradata__drop_otf_table(catalog_integration, relation.identifier, purge_mode) }}

  {{ sql_header if sql_header is not none }}
  {% call statement('main') %}
    CREATE TABLE {{ otf_relation }}
      {% if partitioned_by %}PARTITIONED BY ({{ partitioned_by }}){% endif %}
      {% if sorted_by %}SORTED BY {{ sorted_by }}{% endif %}
      {% if tblproperties %}TBLPROPERTIES({{ tblproperties }}){% endif %}
      AS ({{ sql }}) WITH DATA;
  {% endcall %}
{% endmacro %}
