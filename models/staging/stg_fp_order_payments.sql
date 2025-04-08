with order_payments as (
  select
    order_id,
    payment_sequential,
    lower(payment_type) as payment_type,
    cast(payment_installments as INT64) as payment_installments,
    cast(payment_value as FLOAT64) as payment_value
  from {{ source('fp', 'fp_order_payments') }}
)

select * from order_payments
