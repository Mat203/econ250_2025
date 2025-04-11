{{ config(materialized='table') }}

with order_revenue as (
  select
    order_id,
    sum(oi.price + oi.freight_value) as order_revenue
  from {{ ref('fp_sales_full') }},
  unnest(order_items) as oi
  group by order_id
),

customer_orders as (
  select
    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp,
    orv.order_revenue
  from {{ ref('fp_sales_full') }} o
  left join order_revenue orv using(order_id)
),

customer_analysis as (
  select
    customer_id,
    count(*) as total_orders,
    min(order_purchase_timestamp) as first_order,
    max(order_purchase_timestamp) as last_order,
    sum(order_revenue) as lifetime_value,
    case
      when count(*) = 1 then 'new'
      else 'returning'
    end as customer_type
  from customer_orders
  group by customer_id
)

select *
from customer_analysis
order by lifetime_value desc
