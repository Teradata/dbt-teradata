{#-
This model create the surrogate key table with default members if it doesnt exist or Is_incremental() is False.
It enforces the following conventions in the table structure and content:
  - Key table (model) name: key_{{class}} (eg. key_customer)
  - Surrogate key column name in key table: {{class}}_key (eg. customer_key)
  - Natural key column name in key table: {{class}}_nk (eg. customer_nk)
  - We use the surrogate value -1 in all classes to signify "unknown".

Copy this model and simply rename it key_{{class name}}) to create additional key tables.
#}

{{
  config(
    materialized='incremental',
  )
}}

--Create the surrogate key table with default members if it doesn't exist
select
    -1 (integer) {{ this.table.split('_', 1)[1] }}_key
    ,'UNKNOWN' (varchar(10000)) {{ this.table.split('_', 1)[1] }}_nk
    ,'' (char(100)) domain_cd
    ,current_timestamp created_ts
from (sel 1 a) dummy