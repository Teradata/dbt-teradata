/*
  Build a light integrated modeled layer from source image 
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
*/


{#-
  Not all data has to be integrated and flow all the way from staging through the consumption layer.
  Here we source data directly from an object store and  do minimal preparation 
  to make this dataset usable for discovery.
-#}

{{ config(
    pre_hook=["{{source_object_storage('cloud_storage', 'raw_market_competitors', 
    '/GS/storage.googleapis.com/clearscape_analytics_demo_data/DEMO_Market/Competitor/'
    )}}"]
    )
}}

select new ST_GEOMETRY(ptLocWkt) competitor_location_pt
from {{ ref('stg_market_customers') }} s