{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

with source_data as (
  select
    CAST(datehour as DATE) AS date,
    title,
    views
  from {{ source('test_dataset', 'assignment5_input') }}
)

select
  date,
  title,
  SUM(views) AS views,
  current_timestamp() as insert_time
from source_data
{% if is_incremental() %}
    where date >= DATE_SUB(_dbt_max_partition, interval 1 DAY)
{% endif %}
group by date, title
