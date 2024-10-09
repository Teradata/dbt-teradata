/*
  Build a light integrated modeled layer from source image 
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
  - Surrogate key assigment
  - Naming conventions alignment
*/
{#
  The surrogate key logic (lookup and generation) uses the macros provided in the surrogate_keys.sql file.
  The surrogate keys used by this model only need to be defined in the surrogate_keys dictionnary at the beginning.
  Note that the code below assumes the following conventions, which are not currently enforced in the models generating the key table:
  - Key table name: key_{{class}} (eg. key_customer)
  - Surrogate key column name in key table: {{class}}_key (eg. customer_key)
  - Natural key column name in key table: {{class}}_nk (eg. customer_nk)
  - We use the surrogate value -1 in all classes to signify "unknown".
#}


{%-
  set surrogate_keys={
      'customer':{
        'source_table': 'sim_customers',
        'key_table': 'key_customer',
        'natural_key_cols': ['email'],
        'domain': 'retail',
      }
    }
-%}

{%- set surrogate_keys_hook = generate_surrogate_key_hook(surrogate_keys) -%}

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    pre_hook=surrogate_keys_hook
  )
}}

{#-
In most cases we want to perform a 1:1 projection from source image to lightly integrated, and simply add keys
however we may have to pre-join some tables, mask some columns, apply naming convention
or perform complex transformation logic to derrive natural keys
do this here.
#}
with source as
(
  select
  s.*  
  ,coalesce(c.email,'UNKNOWN') email
  from {{ ref('sim_orders') }} s
  left join {{ ref('sim_customers') }} c
    on c.customer_id=s.user_id
    and c.valid_period contains current_timestamp
)
{#-
The code after this point should generally not require modifications.
#}
select
    s.*
    --Surrogate key columns
    {%- for sk, params in surrogate_keys.items() %}
    ,coalesce({{sk}}.{{ params['key_table'].split('_', 1)[1] }}_key,-1) {{sk}}_key
    {%- endfor %}
from source s
--Surrogate key joins
{%- for sk, params in surrogate_keys.items() %}
left join {{ref(params['key_table'])}} {{sk}}
  on {{sk}}.{{ params['key_table'].split('_', 1)[1] }}_nk={{generate_natural_key(params['natural_key_cols'])}}
  and {{sk}}.domain_cd='{{params['domain']}}'
{% endfor %}