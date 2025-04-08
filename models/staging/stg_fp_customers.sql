with customers as (
  select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    case 
      when customer_city is null or trim(customer_city) = '' then 'UNKNOWN'
      else initcap(customer_city)
    end as customer_city,
    case 
      when customer_state is null or trim(customer_state) = '' then 'UNKNOWN'
      else upper(customer_state)
    end as customer_state
  from {{ source('fp', 'fp_customers') }}
)

select * from customers
