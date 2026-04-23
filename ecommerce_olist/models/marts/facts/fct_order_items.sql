with items as (

    select * from {{ ref('stg_order_items') }}

),

final as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_at,
        price,
        freight_value,

        -- freight ratio: freight cost as a proportion of item price
        case
            when price > 0
            then round((freight_value / price)::numeric, 4)
            else null
        end as freight_ratio

    from items

)

select * from final