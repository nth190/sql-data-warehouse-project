{{ config(
    materialized = 'incremental',
    unique_key   = ['sls_ord_num', 'sls_prd_key'],
    schema       = 'silver_dw'
) }}

with source as (
    select * from {{ source('bronze_dw', 'crm_sales_details') }}
    where sls_ord_num is not null
      and sls_prd_key is not null
      and sls_order_dt is not null

    {% if is_incremental() %}
      and sls_order_dt > (select max(sls_order_dt) from {{ this }})
    {% endif %}
),

deduped as (
    -- keep latest version per order line (sls_ord_num + sls_prd_key)
    select *
    from source
    qualify row_number() over (
        partition by sls_ord_num, sls_prd_key
        order by sls_order_dt desc
    ) = 1
),

cleaned as (
    select
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        current_timestamp() as ingestion_ts
    from deduped
)

select * from cleaned
