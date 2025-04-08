with sellers as (
  select
    seller_id,
    seller_zip_code_prefix,
    case when seller_city is null or trim(seller_city) = '' then 'UNKNOWN'
         else initcap(seller_city)
    end as seller_city,
    case when seller_state is null or trim(seller_state) = '' then 'UNKNOWN'
         else upper(seller_state)
    end as seller_state
  from {{ source('fp', 'fp_sellers') }}
)

select * from sellers
