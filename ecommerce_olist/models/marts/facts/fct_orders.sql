with orders as (

    select * from {{ ref('stg_orders') }}

),

items as (

    select
        order_id,
        sum(price)         as order_value,
        sum(freight_value) as freight_value,
        count(*)           as item_count

    from {{ ref('stg_order_items') }}
    group by order_id

),

customers as (

    select
        customer_id,
        customer_unique_id

    from {{ ref('stg_customers') }}

),

final as (

    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_to_carrier_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,

        -- value metrics
        coalesce(i.order_value, 0)    as order_value,
        coalesce(i.freight_value, 0)  as freight_value,
        coalesce(i.item_count, 0)     as item_count,

        -- delivery metrics
        case
            when o.delivered_to_customer_at is not null
            then extract(day from o.delivered_to_customer_at - o.purchased_at)::int
        end as days_to_deliver,

        case
            when o.delivered_to_customer_at is not null
                 and o.estimated_delivery_at is not null
            then extract(day from o.delivered_to_customer_at - o.estimated_delivery_at)::int
        end as days_late,

        case
            when o.delivered_to_customer_at is not null
                 and o.estimated_delivery_at is not null
            then o.delivered_to_customer_at > o.estimated_delivery_at
        end as is_late,

        -- date dimension foreign key
        o.purchased_at::date as order_date

    from orders o
    left join items i
        on o.order_id = i.order_id
    left join customers c
        on o.customer_id = c.customer_id

)

select * from final