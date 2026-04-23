with reviews as (

    select * from {{ ref('stg_reviews') }}

),

final as (

    select
        review_id,
        order_id,
        review_score,
        review_created_at,
        review_answered_at,

        -- response time in days
        case
            when review_answered_at is not null
                 and review_created_at is not null
            then extract(day from review_answered_at - review_created_at)::int
        end as response_days

    from reviews

)

select * from final