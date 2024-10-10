{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with payments as (

    select * from {{ ref('lim_payments') }}

),

final as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{payment_method}}' then amount_usd else 0 end) as {{payment_method}}_amount_usd,
        {% endfor -%}

        sum(amount_usd) as total_amount_usd

    from payments

    group by 1

)

select * from final
