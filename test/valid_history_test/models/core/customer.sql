{#
This model defines the Customer major entity.
This is a simple projection from the key table, 
technically we could use the key table, but it may contain sensitive information,
and this makes the model on this layer coherent for its users.
#}

locking row for access
select
    customer_key,
    substr(customer_nk,1,3)||'***' email
from {{ ref('key_customer') }}
where domain_cd = 'retail'
