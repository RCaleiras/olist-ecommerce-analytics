with source as (

    select * from {{ source('raw', 'reviews') }}

),

deduplicated as (

    -- 814 duplicate review_ids in the public dataset
    -- keeping the most recent answer timestamp per review_id
    select distinct on (review_id)
        review_id,
        order_id,
        review_score,
        review_creation_date::timestamp  as review_created_at,
        review_answer_timestamp::timestamp as review_answered_at

    from source
    order by review_id, review_answer_timestamp desc

)

select * from deduplicated