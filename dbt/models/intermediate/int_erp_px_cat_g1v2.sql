{{ config(
    materialized = 'incremental',
    unique_key   = 'id',
    schema       = 'silver_dw'
) }}

with source as (
    select * from {{ source('bronze_dw', 'erp_px_cat_g1v2') }}
    where id is not null
      and ingestion_ts is not null

    {% if is_incremental() %}
      and ingestion_ts > (select max(ingestion_ts) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        id,
        trim(cat)         as cat,
        trim(subcat)      as subcat,
        trim(maintenance) as maintenance,
        ingestion_ts
    from source
)

select * from cleaned
