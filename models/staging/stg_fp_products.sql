with products as (
  select
    product_id,
    coalesce(product_category_name, 'UNKNOWN') as product_category_name,
    cast(product_name_lenght as integer) as product_name_length,
    cast(product_description_lenght as integer) as product_description_length,
    cast(product_photos_qty as integer) as product_photos_qty,
    cast(product_weight_g as integer) as product_weight_g,
    cast(product_length_cm as integer) as product_length_cm,
    cast(product_height_cm as integer) as product_height_cm,
    cast(product_width_cm as integer) as product_width_cm,
    cast(product_length_cm as INT64) * cast(product_height_cm as INT64) * cast(product_width_cm as INT64) as product_volume
  from {{ source('fp', 'fp_products') }}
  where product_category_name is not null
)

select * from products
