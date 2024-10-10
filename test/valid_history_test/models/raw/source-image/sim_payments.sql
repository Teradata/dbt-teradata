{#-
This is a source image build, the objective of this model is to mirror 
the current state of the data in the corresponding source system entity.

Here we:
1. "Mirror" the structure of the source (staging) table, filtering out technical columns if needed.
2. Compute delta logic.
3. Persist data accounting for incremental and history management, as needed.
-#}

{#- 
This example is a simple "append" of new records identified in the staging entity.
-#}

{{
  config(
    materialized='incremental',
    incremental_strategy='append'
  )
}}

-- Source image build for `{{this.name.split('_', 1)[1]}}` entity

select source.*
from {{ref('stg_payments')}} source

{%- if var('last_update_ts') and is_incremental() -%}
-- Load is incremental and source has a standard record 
-- landing timestamp, so get delta
where source.{{var('last_update_ts')}} > (select max({{var('last_update_ts')}}) from {{this}})
{% endif %}