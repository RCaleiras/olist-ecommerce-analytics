with source as (

    select * from {{ source('raw', 'payments') }}

),

renamed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value

    from source
    where payment_value >= 0

)

select * from renamed