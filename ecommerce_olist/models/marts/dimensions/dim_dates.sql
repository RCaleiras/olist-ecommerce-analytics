with dates as (

    select generate_series(
        '2017-01-01'::date,
        '2018-08-31'::date,
        interval '1 day'
    )::date as date_day

),

final as (

    select
        date_day                                                    as date,
        extract(year  from date_day)::int                          as year,
        extract(month from date_day)::int                          as month,
        extract(quarter from date_day)::int                        as quarter,
        to_char(date_day, 'Month')                                 as month_name,
        extract(dow from date_day)::int                            as day_of_week,
        to_char(date_day, 'Day')                                   as day_name,
        case when extract(dow from date_day) in (0, 6)
             then true else false end                               as is_weekend,
        case when extract(dow from date_day) not in (0, 6)
             then true else false end                               as is_weekday,
        to_char(date_day, 'YYYY-MM')                               as year_month

    from dates

)

select * from final