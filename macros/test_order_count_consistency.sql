{% test order_count_consistency(model, mart_model) %}
with fp_orders as (
  select count(distinct order_id) as order_count
  from {{ model }}
  where order_status != 'canceled' 
),
mart_orders as (
  select sum(num_orders) as order_count
  from {{ ref(mart_model) }}
)
select *
from fp_orders, mart_orders
where fp_orders.order_count != mart_orders.order_count
{% endtest %}