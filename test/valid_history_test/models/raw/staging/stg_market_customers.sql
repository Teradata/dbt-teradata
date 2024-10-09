{#-
This is a staging model. 
All we do is:
- Point to incoming data (from an external storage or loaded by a tool),
- Add standard control columns for this layer (eg. when was the record received),
#}

{#-
This staging model mirrors a dataset on an object sorage using a foreign table.
There is no materialization strategy defined so the resulting object is a view.
#}

{{ config(
    pre_hook=["{{source_object_storage('cloud_storage', 'raw_market_customers', '/GS/storage.googleapis.com/clearscape_analytics_demo_data/DEMO_Market/Customer/')}}"]
    )
}}

with source as (

    {#-
    Here we reference our a source, defined in the schema.yml
    #}
    select * from {{ source('cloud_storage', 'raw_market_customers') }}

),

renamed as 
(
    select
    source.*
    from source

)

select * from renamed
