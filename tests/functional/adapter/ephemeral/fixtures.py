fct_eph_first_sql = """
-- fct_eph_first.sql
{{ config(materialized='ephemeral') }}

with int_eph_first as(
    select * from {{ ref('int_eph_first') }}
)

select * from int_eph_first
"""

int_eph_first_sql = """
-- int_eph_first.sql
{{ config(materialized='ephemeral') }}

select
    1 as first_column,
    2 as second_column
"""

schema_yml = """
version: 2

models:
  - name: int_eph_first
    columns:
      - name: first_column
        tests:
          - not_null
      - name: second_column
        tests:
          - not_null

  - name: fct_eph_first
    columns:
      - name: first_column
        tests:
          - not_null
      - name: second_column
        tests:
          - not_null

"""
