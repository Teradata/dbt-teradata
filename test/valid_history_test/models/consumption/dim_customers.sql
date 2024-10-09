with customers as (
    {# 
    Here we use the key table as a major entity 
    this would work from the light integrated lim_customers entity,
    but we could have several customer sources to gather and de-duplicate, 
    our key table does this job.
    Note that in real life we may want to secure the key table (PII contained in natural keys) 
    and prevent locking... So we would define a projection in discovery or core layers.
    #}
    select * from {{ ref('key_customer') }}
    where domain_cd in ('retail', 'business')

),

customer_orders as (
    {# We can directly aggregate the orders from the LIM layer #}
    select
        customer_key,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(id) as number_of_orders
    from {{ ref('lim_orders') }}
    group by 1

),

customer_lifetime_value as (
    {# customer_lifetime_value is a common metric, and it is historized. #} 
    select * from {{ ref('customer_lifetime_value') }}
    where customer_lifetime_value.valid_period contains current_date

),

final as (

    select
        customers.customer_key,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_lifetime_value.total_amount_usd as customer_lifetime_value

    from customers

    left join customer_orders on customers.customer_key = customer_orders.customer_key
    left join customer_lifetime_value on customers.customer_key = customer_lifetime_value.customer_key

)

select * from final