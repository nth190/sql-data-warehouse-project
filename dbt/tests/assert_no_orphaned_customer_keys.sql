-- Assert that there are no orphaned sales records (fact without valid dimension)

-- Test for missing customer dimensions
select
    f.sales_key,
    f.order_number,
    f.customer_key,
    'Missing customer dimension' as error_message
from {{ ref('fact_sales') }} f
left join {{ ref('dim_customers') }} c 
    on f.customer_key = c.customer_key
where c.customer_key is null
