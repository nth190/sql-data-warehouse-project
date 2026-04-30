{{ config(
    materialized = 'view',
    schema = 'silver_dw'
) }}

with crm_customers as (
    select * from {{ ref('int_crm_cust_info') }}
),

erp_customers as (
    select * from {{ ref('int_erp_cust_az12') }}
),

locations as (
    select * from {{ ref('int_erp_loc_a101') }}
),

enriched as (
    select
        c.cst_id,
        c.cst_key,
        c.cst_firstname,
        c.cst_lastname,
        c.cst_marital_status,
        case
            when c.cst_gndr != 'n/a' then c.cst_gndr
            else coalesce(e.gen, 'n/a')
        end as cst_gndr,
        e.bdate,
        l.cntry as country_code,
        l.cntry as country_name,
        l.cntry as region,
        'standard' as segment,
        c.cst_create_date,
        c.ingestion_ts
    from crm_customers c
    left join erp_customers e on c.cst_id = cast(e.cid as unsigned)
    left join locations l on c.cst_id = cast(l.cid as unsigned)
)

select * from enriched
