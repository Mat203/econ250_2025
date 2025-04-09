{{ config(materialized='table') }}

with seller_orders as (
  select
    o.order_id,
    o.order_purchase_timestamp,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    oi.seller_id
  from {{ ref('fp_sales_full') }} o
  join {{ ref('stg_fp_order_items') }} oi
    on o.order_id = oi.order_id
  left join {{ ref('stg_fp_sellers') }} s
    on oi.seller_id = s.seller_id
  where o.order_status != 'canceled'
),

aggregated_by_seller as (
  select
    seller_id,
    count(distinct order_id) as num_orders,
    round(avg(timestamp_diff(order_delivered_carrier_date, order_purchase_timestamp, DAY)), 2) as avg_time_to_ship_days,
    round(avg(timestamp_diff(order_delivered_customer_date, order_purchase_timestamp, DAY)), 2) as avg_time_to_deliver_days
  from seller_orders
  group by seller_id
)

select * from aggregated_by_seller
order by avg_time_to_ship_days desc