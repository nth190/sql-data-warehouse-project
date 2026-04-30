{{ config(
    materialized = 'incremental',
    unique_key = 'sales_key',
    schema = 'gold_dw',
    tags = ['gold', 'fact']
) }}

with sales_data as (
    select * from {{ ref('int_sales') }}
),

dim_customers as (
    select
        customer_key,
        customer_id,
        valid_from,
        valid_to,
        is_current
    from {{ ref('dim_customers') }}
),

dim_products as (
    select
        product_key,
        product_number,
        product_id,
        valid_from,
        valid_to,
        is_current
    from {{ ref('dim_products') }}
),

-- Join sales with dimension tables using SCD2 logic
fact_data as (
    select
        MD5(CONCAT(
            COALESCE(CAST(s.sls_ord_num AS CHAR), ''),
            COALESCE(CAST(s.sls_prd_key AS CHAR), ''),
            COALESCE(CAST(s.sls_cust_id AS CHAR), '')
        )) as sales_key,
        s.sls_ord_num as order_number,
        s.sls_order_dt as order_date,
        
        -- Get the customer_key valid at the time of the sale (SCD2 lookup)
        c.customer_key,
        
        -- Get the product_key valid at the time of the sale (SCD2 lookup)
        p.product_key,
        
        s.sls_quantity as quantity,
        s.sls_price as amount,
        s.sls_quantity * s.sls_price as total_amount,
        
        current_timestamp() as created_at,
        '{{ invocation_id }}' as batch_id
        
    from sales_data s
    
    -- SCD2 join for customers - find the version valid at order_date
    left join dim_customers c
        on s.sls_cust_id = c.customer_id
        and s.sls_order_dt >= c.valid_from
        and s.sls_order_dt < c.valid_to
    
    -- SCD2 join for products - find the version valid at order_date
    left join dim_products p
        on s.sls_prd_key = p.product_number
        and s.sls_order_dt >= p.valid_from
        and s.sls_order_dt < p.valid_to
    
    {% if is_incremental() %}
    where s.sls_order_dt > (select max(order_date) from {{ this }})
    {% endif %}
)

select * from fact_data
