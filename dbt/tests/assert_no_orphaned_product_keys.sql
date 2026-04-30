-- Assert that there are no orphaned sales records for products

select
    f.sales_key,
    f.order_number,
    f.product_key,
    'Missing product dimension' as error_message
from {{ ref('fact_sales') }} f
left join {{ ref('dim_products') }} p 
    on f.product_key = p.product_key
where p.product_key is null
