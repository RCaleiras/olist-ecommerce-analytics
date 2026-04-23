with source as (

    select * from {{ source('raw', 'products') }}

),

translation as (

    select * from {{ source('raw', 'translation') }}

),

renamed as (

    select
        p.product_id,
        coalesce(p.product_category_name, 'unknown') as product_category_name,
        coalesce(
            t.product_category_name_english,
            case p.product_category_name
                when 'pc_gamer'
                    then 'PC Gamer'
                when 'portateis_cozinha_e_preparadores_de_alimentos'
                    then 'Portable Kitchen & Food Processors'
                else 'unknown'
            end
        )                                             as product_category_name_en,
        coalesce(p.product_weight_g, 0)               as product_weight_g,
        coalesce(p.product_length_cm, 0)              as product_length_cm,
        coalesce(p.product_height_cm, 0)              as product_height_cm,
        coalesce(p.product_width_cm, 0)               as product_width_cm

    from source p
    left join translation t
        on p.product_category_name = t.product_category_name

)

select * from renamed