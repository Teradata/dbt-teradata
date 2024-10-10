{#-
Generic source image build from staging table, two blocks:
1. We use a common macro build_source_image() that
  "mirrors" the structure of the source table, 
  accounting for history preparation and column filtering, if specified.
2. The delta logic is directly defined in the model, 
  based on the nature of the source, however we provide a boilerplate code 
  for historized and non-historized source images using the project default 
  "last update timestamp", assuming that it is present in the source tables if set.

Model Usage:
1. Update the config parameters (all optional):
    - exclude_columns: List of columns to exclude, if any
    - If needed: materialized and incremental_strategy
    - If materialized='incremental' and incremental_strategy in ('delete+insert', 'merge', 'valid_history')
      - unique_key: Primary key in the source table (temporal column excluded) 
    - If materialized='incremental' and incremental_strategy = 'valid_history'
      - valid_from: Name of the column indicating the record change date or time in the source system (If None and this is , then use current timestamp)
      - valid_period: Name of the column indicating the record valid period in the model
    - disable_delta: Set in order to disable the delta logic.

2. Update source_table (name of the staging table we build from) parameter in the macro, the others are infered from the configuration

Possible further customization: 
  - infer the source table name from the current model name (by simply changing the suffix) in the  build_source_image macro.
  - test the use on_schema_change='sync_all_columns'
-#}

{#- 
This example uses an incremental "valid_history" strategy to reflect 
the historical image of the mirrored source system entity.
This enables us to "go back in time" (in source system time terms) to,
for example, compute historical metrics or rebuild downstream history without reloading any data.
This also enables us to handle back-dated corrections from the source systems 
without having to re-build the entire history forward.
-#}

{{
  config(
    unique_key=['customer_id'],
    valid_from='last_update_ts',
    valid_period='valid_period',
    materialized='incremental',
    incremental_strategy='valid_history',
    use_valid_to_time='no',
    enable_deltas='yes'
  )
}}

-- Generic source image build for `{{this.name.split('_', 1)[1]}}` entity
{{ build_source_image (source_table='stg_customers', config=config) }}

{%- if var('last_update_ts') and is_incremental() and not config.get('disable_delta')-%}
-- Load is incremental and source has a standard record 
-- landing timestamp, so get delta
where source.{{var('last_update_ts')}} > 
  {%- if config.get('valid_period') -%}
    (select max(begin({{config.get('valid_period')}})) from {{this}})
  {%- else -%}
    (select max({{var('last_update_ts')}}) from {{this}})
  {%- endif -%}
{% endif %}