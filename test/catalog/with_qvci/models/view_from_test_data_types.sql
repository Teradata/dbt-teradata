{{
    config(
        materialized='view'
    )
}}
SELECT * FROM {{ ref('test_data_types') }}
