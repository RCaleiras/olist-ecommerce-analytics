with source as (

    select * from {{ source('raw', 'geolocation') }}

),

filtered as (

    -- remove duplicate rows and coordinates outside Brazil bounding box
    select distinct
        geolocation_zip_code_prefix as zip_code_prefix,
        geolocation_lat             as lat,
        geolocation_lng             as lng,
        geolocation_city            as city,
        geolocation_state           as state

    from source
    where
        geolocation_lat between -34 and 6
        and geolocation_lng between -74 and -34

),

aggregated as (

    -- multiple coordinates per ZIP prefix — aggregate to median lat/lng
    select
        zip_code_prefix,
        percentile_cont(0.5) within group (order by lat) as lat,
        percentile_cont(0.5) within group (order by lng) as lng,
        max(city)                                         as city,
        max(state)                                        as state

    from filtered
    group by zip_code_prefix

)

select * from aggregated