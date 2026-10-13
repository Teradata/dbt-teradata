{{ config(materialized='table') }}
-- SQL model consuming a python model (python -> sql mixed DAG)
select
    customer_id,
    total_amount,
    n_orders
from {{ ref('py_aggregate') }}
where n_orders >= 1
