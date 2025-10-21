/*
  Build a light integrated modeled layer from source image 
  for entity `{{this.name.split('_', 1)[1]}}`, performing: 
  - Data domain aligments (eg. using standard data types, units conversions, codes standardization...)
  - Naming conventions alignment
*/


{#-
In most cases we want to perform a 1:1 projection from source image to lightly integrated, and simply add keys
however we may have to pre-join some tables, mask some columns, apply naming convention
or perform complex transformation logic to derrive natural keys
do this here.
#}

sel
  id as payment_id,
  order_id,
  payment_method,
  --`amount` is currently stored in cents, so we convert it to dollars
  amount / 100 as amount_usd
  from {{ ref('sim_payments') }} s
