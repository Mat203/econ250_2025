with product_categories as (
  select
    product_category_name,
    product_category_name_english
  from {{ source('fp', 'fp_product_category_name_translation') }}
  where product_category_name is not null
)

select * from product_categories