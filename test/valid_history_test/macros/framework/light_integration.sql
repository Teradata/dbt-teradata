{%- macro build_light_integrated(source_table, surrogate_keys) -%}
{#/*
  This macro takes a source entity and adds surrogate keys.
  The surrogate key logic (lookup and generation) uses the macros provided in the surrogate_keys.sql file.
  The surrogate keys used by this model only need to be defined in the surrogate_keys dictionnary at the beginning.
  Note that the code below assumes the following conventions, which are not currently enforced in the models generating the key table:
  - Key table name: key_{{class}} (eg. key_customer)
  - Surrogate key column name in key table: {{class}}_key (eg. customer_key)
  - Natural key column name in key table: {{class}}_nk (eg. customer_nk)
  - We use the surrogate value -1 in all classes to signify "unknown".
*/#}

{# This will remove all natural key columns from the model maeked with mask=True (eg. PII protection) #}
{%- set exclude_columns = list_natural_key_cols(surrogate_keys) -%}


{#-/*
In most cases we want to perform a 1:1 projection from source image to lightly integrated, and simply add keys
however we may have to pre-join some tables, mask some columns, apply naming convention
or perform complex transformation logic to derrive natural keys
do this here.
*/#}

with source as
(
  select
  s.*
  from {{ source_table }} s
  where valid_period contains current_timestamp
)
{#-The code after this point should generally not require modifications.#}
select
    --Value columns
    {%- for column in adapter.get_columns_in_relation(source_table)  if column.name not in exclude_columns %}
      {{ column.name }}{{ "," if not loop.last else "" }}
    {%- endfor %}
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

{% endmacro %}
