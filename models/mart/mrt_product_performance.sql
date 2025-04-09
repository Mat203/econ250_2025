{{ config(materialized='table') }}

with product_performance as (
  select
    p.product_category_name,
    pct.product_category_name_english,
    sum(oi.price + oi.freight_value) as total_revenue,
    count(distinct o.order_id) as num_orders
  from {{ ref('fp_sales_full') }} o,
       unnest(order_items) as oi
  left join {{ ref('stg_fp_products') }} p
    on oi.product_id = p.product_id
  left join {{ ref('stg_fp_product_category_name_translation') }} pct
    on p.product_category_name = pct.product_category_name
  group by 
    p.product_category_name,
    pct.product_category_name_english
)

select
  product_category_name,
  product_category_name_english,
  round(total_revenue, 2) as total_revenue,
  num_orders
from product_performance
order by total_revenue desc
limit 50