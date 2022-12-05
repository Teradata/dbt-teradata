table_with_cte_sql="""
{{
            config(
                materialized="table"
            )
        }}
        WITH
        source_a AS (
            SELECT 1 AS a1, 2 AS a2
        ),
        source_b AS (
            SELECT 1 AS b1, 2 AS b2
        )
        SELECT * FROM source_a, source_b
"""

table_without_cte_sql="""
{{
            config(
                materialized="table"
            )
        }}
        SELECT 1 AS a1, 2 AS a2, 1 AS b1, 2 AS b2
"""

view_with_cte_sql="""
{{
            config(
                materialized="view"
            )
        }}
        WITH
        source_a AS (
            SELECT 1 AS a1, 2 AS a2
        ),
        source_b AS (
            SELECT 1 AS b1, 2 AS b2
        )
        SELECT * FROM source_a, source_b
"""

view_without_cte_sql="""
{{
            config(
                materialized="view"
            )
        }}
        WITH
        source_a AS (
            select 1 AS a1, 2 AS a2
        ),
        source_b AS (
            select 1 AS b1, 2 AS b2
        )
        SELECT * FROM source_a, source_b
"""

table_with_cte_comments_sql="""
{{
                  config(
                      materialized="table"
                  )
              }}
              -- This is a test comment
              WITH
              source_a AS (
                  SELECT 1 AS a1, 2 AS a2
              ),
              source_b AS (
                  SELECT 1 AS b1, 2 AS b2
              )
              SELECT * FROM source_a, source_b
"""

schema_yaml="""
version: 2
models:
  - name: table_with_cte
    columns:
    - name: a1
      tests:
        - not_null
    - name: a2
      tests:
        - not_null
    - name: b1
      tests:
        - not_null
    - name: b2
      tests:
        - not_null
  - name: table_without_cte
    columns:
    - name: a1
      tests:
        - not_null
    - name: a2
      tests:
        - not_null
    - name: b1
      tests:
        - not_null
    - name: b2
      tests:
        - not_null
"""

test_table_csv="""
id,attrA,attrB
1,val1A,val1B
2,val2A,val2B
3,val3A,val3B
4,val4A,val4B
""".lstrip()

table_from_source_sql="""
SELECT * FROM {{ source('alias_source_schema', 'alias_source_table') }}
"""


table_from_source_for_catalog_test_sql="""
        {{
            config(
                materialized="table"
            )
        }}
    SELECT * FROM {{ source('alias_source_schema', 'alias_source_table') }}
"""

view_from_source_for_catalog_test_sql="""
        {{
            config(
                materialized="view"
            )
        }}
        SELECT * FROM {{ source('alias_source_schema', 'alias_source_table') }}
"""


sources_yml="""
version: 2
sources:
  - name: alias_source_schema
    schema: "{{ target.schema }}"
    tables:
      - name: alias_source_table
        identifier: test_table
"""


###################################################################################################################                    
# For create_table Test
##################################################################################################################

test_table_in_create_test_csv="""
id,attrA,attrB,create_date
1,val1A,val1B,2020-03-05
2,val2A,val2B,2020-04-05
3,val3A,val3B,2020-05-05
4,val4A,val4B,2020-10-05
""".lstrip()



create_table_with_cte_sql="""
        {{
            config(
                materialized="table"
            )
        }}
        WITH
        source_a AS (
            select 1 AS id, 'val1A' AS attrA, 'val1B' AS attrB
        ),
        source_b AS (
            SELECT to_date('2020-03-05') AS create_date
        )
        SELECT * FROM source_a, source_b
"""

table_no_config_sql="""
        {{
            config(
                materialized="table"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

table_with_table_kind_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_table_kind_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_table_option_sql="""
        {{
            config(
                materialized="table",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_table_option_sql="""
        {{
            config(
                materialized="table",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_table_kind_and_table_option_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""


from_cte_table_with_table_kind_and_table_option_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_many_table_options_sql="""
        {{
            config(
                materialized="table",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON,
                  NO MERGEBLOCKRATIO,
                  WITH CONCURRENT ISOLATED LOADING FOR ALL"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_many_table_options_sql="""
        {{
            config(
                materialized="table",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON,
                  NO MERGEBLOCKRATIO,
                  WITH CONCURRENT ISOLATED LOADING FOR ALL"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_statistics_sql="""
        {{
            config(
                materialized="table",
                with_statistics="true"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_statistics_sql="""
        {{
            config(
                materialized="table",
                with_statistics="true"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_options_and_statistics_sql="""
        {{
            config(
                materialized="table",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON,
                  NO MERGEBLOCKRATIO,
                  WITH CONCURRENT ISOLATED LOADING FOR ALL",
                with_statistics="true"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_options_and_statistics_sql="""
        {{
            config(
                materialized="table",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON,
                  NO MERGEBLOCKRATIO,
                  WITH CONCURRENT ISOLATED LOADING FOR ALL",
                with_statistics="true"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_index_sql="""
        {{
            config(
                materialized="table",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_index_sql="""
        {{
            config(
                materialized="table",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""


table_with_larger_index_sql="""
        {{
            config(
                materialized="table",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_larger_index_sql="""
        {{
            config(
                materialized="table",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""


table_with_kind_options_index_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)
                      INDEX index_attrA (attrA) WITH LOAD IDENTITY"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_kind_options_index_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)
                      INDEX index_attrA (attrA) WITH LOAD IDENTITY"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

table_with_kind_options_stats_index_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON",
                with_statistics="true",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)
                      INDEX index_attrA (attrA) WITH LOAD IDENTITY"
            )
        }}
        SELECT * FROM {{ ref('test_table_in_create_test') }}
"""

from_cte_table_with_kind_options_stats_index_sql="""
        {{
            config(
                materialized="table",
                table_kind="multiset",
                table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON",
                with_statistics="true",
                index="PRIMARY INDEX(id)
                PARTITION BY RANGE_N(create_date
                              BETWEEN DATE '2020-01-01'
                              AND     DATE '2021-01-01'
                              EACH INTERVAL '1' MONTH)
                      INDEX index_attrA (attrA) WITH LOAD IDENTITY"
            )
        }}
        SELECT * FROM {{ ref('create_table_with_cte') }}
"""

########################################################################################################################


table_for_case_sensitivity_sql="""
{{
            config(
              materialized="table",
              schema="DBT_TEST"
            )
        }}

        SELECT * FROM dbc.dbcinfo
"""


data_with_timestamp_sql="""
{{
          config(
            materialized="table"
          )
        }}
        SELECT '100' AS important_data, current_timestamp - INTERVAL '1' DAY AS timestamp_column
"""

teradata_freshness_sources_yml="""
version: 2

sources:
  - name: validate_teradata_freshness_test
    schema: "{{ target.schema }}"
    database: "{{ target.schema }}"
    freshness: # default freshness
      warn_after: {count: 25, period: hour}
    loaded_at_field: timestamp_column
    tables:
      - name: data_with_timestamp
"""


test_table_for_type_inference_csv="""
id,timestamp_column,date_column,float_column,integer_column,boolean_column
1,2022-01-13 13:04:34,2022-01-13,10.03,10234234,true
2,2022-02-13 13:14:34,2022-02-13,11.03,10234234,true
3,2022-03-13 13:24:34,2022-03-13,12.03,10234234,true
4,2022-04-13 13:34:34,2022-04-13,13.03,10234234,true
5,2022-05-13 13:44:34,2022-05-13,14.03,10234234,true
6,2022-06-13 13:54:34,2022-06-13,15.03,10234234,false
7,2022-07-13 14:04:34,2022-07-13,16.03,10234234,false
8,2022-08-13 14:24:34,2022-08-13,17.03,10234234,false
9,2022-09-13 14:54:34,2022-09-13,18.0,10234234,false
0,2022-10-13 16:24:34,2022-10-13,19.0343,10234234,false
""".lstrip()


test_table_in_timestamp_macro_test_csv="""
id,attrA,attrB,create_date
1,val1A,val1B,2020-03-05
2,val2A,val2B,2020-04-05
3,val3A,val3B,2020-05-05
4,val4A,val4B,2020-10-05
""".lstrip()



test_table_snapshot_sql="""
{% snapshot orders_snapshot %}

{{ config(
  check_cols=['create_date'],
  unique_key='id',
  strategy='check',
  target_schema='dbt_test_table_snapshot_schema'
) }}
SELECT * FROM {{ ref('test_table_in_timestamp_macro_test') }}

{% endsnapshot %}
"""

# override this macro to simulate specific time with full seconds
macros_sql="""
{% macro teradata__current_timestamp() -%}
  to_timestamp_tz('2022-01-27 15:15:21.000000-05:00')
{%- endmacro %}
"""

#model
dbcinfo_sql="""
{{
    config(
        materialized="table",
        schema="DBT_TEST_IS_THE_BEST"
        )
}}
        SELECT * FROM dbc.dbcinfo
"""

#macro
generate_schema_name_sql="""
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {% else %}
    {{ custom_schema_name | trim | lower }}
  {%- endif -%}
{%- endmacro %}
"""
