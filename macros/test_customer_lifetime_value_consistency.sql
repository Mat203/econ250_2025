{% test customer_lifetime_value_consistency(model, mart_model) %}

with source_ltv as (
    select 
        o.customer_id,
        sum(oi.price + oi.freight_value) as total_revenue
    from {{ ref('fp_sales_full') }} o,
         unnest(order_items) as oi
    group by o.customer_id
),

mart_ltv as (
    select 
        customer_id,
        lifetime_value
    from {{ ref(mart_model) }}
)

select 
    sl.customer_id,
    sl.total_revenue,
    ml.lifetime_value
from source_ltv sl
join mart_ltv ml 
  on sl.customer_id = ml.customer_id
where ABS(sl.total_revenue - ml.lifetime_value) > 0.01

{% endtest %}
