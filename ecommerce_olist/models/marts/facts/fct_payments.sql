with payments as (

    select * from {{ ref('stg_payments') }}

),

final as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value

    from payments

)

select * from final