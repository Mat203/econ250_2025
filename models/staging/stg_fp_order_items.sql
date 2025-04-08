with order_items as (
  select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price as FLOAT64) as price,
    cast(freight_value as FLOAT64) as freight_value
  from {{ source('fp', 'fp_order_items') }}
)

select * from order_items
