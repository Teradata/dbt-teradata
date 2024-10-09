{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('lim_orders') }}

),

order_payments as (

    select * from {{ ref('order_payments') }}

),

final as (

    select
        orders.customer_key,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        order_payments.{{payment_method}}_amount_usd,

        {% endfor -%}

        order_payments.total_amount_usd as amount

    from orders

    left join order_payments on orders.id = order_payments.order_id

)

select * from final
