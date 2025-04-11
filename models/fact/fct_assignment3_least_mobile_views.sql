{{ config(materialized='table') }}

with cte as (
    select
        hour_of_day,
        device_type,
        views
    from {{ ref('stg_assignment3_uk_wiki') }}
    where title = 'AWStats'
)

select
    hour_of_day,
    sum(views) as total_views,
    sum(case when device_type = 'mobile' then views else 0 end) as total_mobile_views
from cte
group by hour_of_day
order by hour_of_day
