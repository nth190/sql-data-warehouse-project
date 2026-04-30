-- Assert that each product has only ONE current version

select
    product_id,
    count(*) as current_version_count
from {{ ref('dim_products') }}
where is_current = true
group by product_id
having count(*) > 1
