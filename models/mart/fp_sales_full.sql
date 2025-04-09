with orders as (
  select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    delivery_duration_days,
    is_shipped
  from {{ ref('stg_fp_orders') }}
),

aggregated_items as (
  select 
    oi.order_id,
    array_agg(
      struct(
        oi.order_item_id as order_item_id,
        oi.product_id as product_id,
        prod.product_category_name as product_category,
        pct.product_category_name_english as product_category_english,
        oi.price as price,
        oi.freight_value as freight_value,
        (oi.price + oi.freight_value) as item_total_value
      )
      order by oi.order_item_id
    ) as order_items
  from {{ ref('stg_fp_order_items') }} oi
  left join {{ ref('stg_fp_products') }} prod 
    on oi.product_id = prod.product_id
  left join {{ ref('stg_fp_product_category_name_translation') }} pct 
    on prod.product_category_name = pct.product_category_name
  group by oi.order_id
),

aggregated_payments as (
  select 
    order_id,
    array_agg(
      struct(
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
      )
      order by payment_sequential
    ) as payments
  from {{ ref('stg_fp_order_payments') }}
  group by order_id
)

select
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  o.delivery_duration_days,
  o.is_shipped,
  c.customer_zip_code_prefix,
  c.customer_city,
  c.customer_state,
  ai.order_items,
  ap.payments
from orders o
left join {{ ref('stg_fp_customers') }} c
  on o.customer_id = c.customer_id
left join aggregated_items ai 
  on o.order_id = ai.order_id
left join aggregated_payments ap
  on o.order_id = ap.order_id
