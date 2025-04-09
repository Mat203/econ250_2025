with orders as (
  select
    order_id,
    customer_id,
    lower(order_status) as order_status,
    cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(order_approved_at as timestamp) as order_approved_at,
    cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
    cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
    date_diff(cast(order_delivered_customer_date as date), cast(order_purchase_timestamp as date), DAY) as delivery_duration_days,
    case 
      when lower(order_status) = 'delivered' then true
      else false
    end as is_shipped
  from {{ source('fp', 'fp_orders') }}
  where order_purchase_timestamp is not null
    and order_delivered_customer_date is not null
    and order_estimated_delivery_date is not null
    and order_approved_at is not null
    and order_delivered_carrier_date is not null
    and order_delivered_customer_date is not null
    and order_delivered_customer_date is not null
)

select * from orders
