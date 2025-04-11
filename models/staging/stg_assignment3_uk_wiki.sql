{{ config(
    materialized = 'view'
) }}

with uk as (
    select
        'desktop' as device_type,
        cast(datehour as date) as date,
        case 
            when extract(DAYOFWEEK from datehour) = 1 then 7 
            else extract(DAYOFWEEK from datehour) - 1 
        end as day_of_week,
        extract(HOUR from datehour) as hour_of_day,
        title,
        views
    from {{ source('test_dataset', 'assignment3_input_uk') }}
),

mobile as (
    select
        'mobile' as device_type,
        cast(datehour as date) as date,
        case 
            when extract(DAYOFWEEK from datehour) = 1 then 7 
            else extract(DAYOFWEEK from datehour) - 1 
        end as day_of_week,
        extract(HOUR from datehour) as hour_of_day,
        title,
        views
    from {{ source('test_dataset', 'assignment3_input_uk_m') }}
)

select * from uk
union all
select * from mobile
