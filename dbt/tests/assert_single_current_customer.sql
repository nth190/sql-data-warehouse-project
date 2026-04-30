-- Assert that each customer has only ONE current version

select
    customer_id,
    count(*) as current_version_count
from {{ ref('dim_customers') }}
where is_current = true
group by customer_id
having count(*) > 1
