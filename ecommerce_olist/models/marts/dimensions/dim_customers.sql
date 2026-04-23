with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

first_order as (

    select
        customer_id,
        min(purchased_at)::date as first_order_date

    from orders
    group by customer_id

),

joined as (

    select
        c.customer_unique_id,
        c.customer_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        f.first_order_date

    from customers c
    left join first_order f
        on c.customer_id = f.customer_id

),

deduplicated as (

    -- one row per customer_unique_id — keep the record with the earliest first_order_date
    select distinct on (customer_unique_id)
        customer_unique_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        first_order_date

    from joined
    order by customer_unique_id, first_order_date asc

)

select * from deduplicated