/*
  Build a light integrated modeled layer from source image 
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
*/


{#-
  Not all data has to be integrated and flow all the way from staging through the consumption layer.
  Here we source data directly from staging and  do minimal preparation 
  to make this dataset usable for discovery.
-#}

select
spend,
nearby5,
nearby10,
new ST_GEOMETRY(ptLocWkt) location_pt
from {{ source('cloud_storage', 'raw_market_customers') }} s