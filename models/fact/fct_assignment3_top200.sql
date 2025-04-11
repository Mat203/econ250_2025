{{ config(materialized='table') }}

with cte as (
    select
        title,
        sum(views) as total_views,
        sum(case when device_type = 'mobile' then views else 0 end) as total_mobile_views
    from {{ ref('int_assignment3_uk_wiki') }}
    where is_meta_page = false      
    group by title
),

top200 as (
    select
        title,
        total_views,
        total_mobile_views,
        total_mobile_views/total_views * 100 as mobile_percentage
    from cte
    order by total_views desc
    limit 200                          
)

select
    title,
    total_views,
    total_mobile_views,
    format('%.2f%%', mobile_percentage) as mobile_percentage
from top200
order by mobile_percentage asc       
