with payments as (

    select * from {{ ref('lim_payments') }}

),

final as (

    select
        payment_method
        ,sum(amount_usd) as total_amount
        ,count(1) as frequency_count
    from payments
    group by 1

)

select * from final