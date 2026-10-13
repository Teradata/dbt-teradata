{{ config(materialized='table') }}

select
    1 as example_id,
    {{ get_audit_columns() }}
