{{ config(
    materialized = 'view',
    schema = 'silver_dw'
) }}

with products as (
    select * from {{ ref('int_crm_prd_info') }}
),

categories as (
    select * from {{ ref('int_erp_px_cat_g1v2') }}
),

enriched as (
    select
        p.prd_id,
        p.prd_key,
        p.prd_nm,
        p.prd_cost,
        p.prd_line,
        p.prd_start_dt,
        p.prd_end_dt,
        c.cat,
        c.subcat,
        c.maintenance,
        p.ingestion_ts
    from products p
    left join categories c
        on p.cat_id = c.id
)

select * from enriched
