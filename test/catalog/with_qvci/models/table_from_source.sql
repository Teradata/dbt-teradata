{{
    config(
        materialized='table'
    )
}}
SELECT * FROM {{ source('alias_source_schema', 'alias_source_table') }}
