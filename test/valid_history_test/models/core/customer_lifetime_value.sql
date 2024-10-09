{#
Here we compute the customer historical lifetime value metric.
The timeline of this metric is driven by the order dates (since the value increases at every order).
If this is a full load, we build the entire timeline from the orers history.
#}

{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    incremental_strategy='valid_history',
    valid_period='valid_period'

  )
}}

select
    orders.customer_key,
{%- if is_incremental() %}
    --Incremental load: compute the customer value as of date
    sum(amount_usd) as total_amount_usd,
    period(max(order_date), ('9999-12-31' (date))) valid_period
{%- else %}
    --Full load: compute the historical customer value history    
    sum(sum(amount_usd)) over(partition by orders.customer_key order by order_date ROWS UNBOUNDED PRECEDING) total_amount_usd,
    period(
        order_date, 
        coalesce(lead(order_date) over(partition by orders.customer_key order by order_date) , ('9999-12-31' (date)))
        ) valid_period
{%- endif %}

from {{ ref('lim_payments') }} payments
left join  {{ ref('lim_orders') }} orders on payments.order_id = orders.id

{% if is_incremental() %}
group by customer_key
{% else %}
group by orders.customer_key, order_date
{% endif %}
