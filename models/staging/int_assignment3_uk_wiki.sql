{{ config(
    materialized = 'view'
) }}

with base as (
    select
        stg.*,
        case
            when regexp_contains(title, r':') then regexp_extract(title, r'^(.*?):', 1)
            else null
        end as meta_page_type
    from {{ ref('stg_assignment3_uk_wiki') }} as stg
)

select
    base.*,
    case
        when meta_page_type in (
            'Файл',
            'Обговорення',
            'Обговорення_Користувача',
            'Вікіпедія',
            'Категорія',
            'Шаблон',
            'Користувач',
            'Користувачка',
            'Портал',
            'Обговорення_файлу',
            'Спеціальна'
        ) then true
        else false
    end as is_meta_page
from base
