{{ config(materialized='table') }}
select customer_id, customer_name, region
from {{ ref('raw_customers') }}
