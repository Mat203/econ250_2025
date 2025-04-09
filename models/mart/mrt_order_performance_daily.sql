{{ config(materialized='table') }}

with order_revenue as (
  select
    order_id,
    sum(item_total_value) as order_revenue
  from {{ ref('fp_sales_full') }},
  unnest(order_items) as oi
  group by order_id
),

daily as (
  select
    DATE(order_purchase_timestamp) as order_date,
    order_status,
    count(distinct order_id) as num_orders,
    round(sum(coalesce(orv.order_revenue, 0)), 2) as total_revenue
  from {{ ref('fp_sales_full') }} o
  left join order_revenue as orv using(order_id)
  where lower(order_status) != 'canceled'
  group by 1, 2
)

select * from daily