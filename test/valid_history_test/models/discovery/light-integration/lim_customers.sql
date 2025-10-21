/*
  Build a light integrated modeled layer from source image 
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
  - Surrogate key assigment
  - Naming conventions alignment
*/

{#/*
  The surrogate key logic (lookup and generation) uses the macros provided in the surrogate_keys.sql file.
  The surrogate keys used by this model only need to be defined in the surrogate_keys dictionnary at the beginning.
  Note that the code below assumes the following conventions, which are not currently enforced in the models generating the key table:
  - Key table name: key_{{class}} (eg. key_customer)
  - Surrogate key column name in key table: {{class}}_key (eg. customer_key)
  - Natural key column name in key table: {{class}}_nk (eg. customer_nk)
  - We use the surrogate value -1 in all classes to signify "unknown".
*/#}


{# Define the surrogate keys here #}
{%-
  set surrogate_keys={
      'customer':{
        'source_table': 'sim_customers',
        'key_table': 'key_customer',
        'natural_key_cols': ['email'],
        'domain': 'retail',
      },
      'family':{
        'source_table': 'sim_customers',
        'key_table': 'key_customer',
        'natural_key_cols': ['last_name'],
        'domain': 'families',
        'mask': True
      }
    }
-%}

{# Define the apply strategy and generate key in pre-hook #}
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='customer_key',
    pre_hook=generate_surrogate_key_hook(surrogate_keys)
  )
}}

{#-
In most cases we want to perform a 1:1 projection from source image to lightly integrated, 
and simply add keys. This macro does just that.
#}
{{build_light_integrated( ref('sim_customers'), surrogate_keys)}}