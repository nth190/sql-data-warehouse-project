{{ config(
    materialized = 'incremental',
    unique_key   = 'cid',
    schema       = 'silver_dw'
) }}

with source as (
    select * from {{ source('bronze_dw', 'erp_loc_a101') }}
    where cid is not null
      and ingestion_ts is not null

    {% if is_incremental() %}
      and ingestion_ts > (select max(ingestion_ts) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        cid,
        trim(cntry) as cntry,
        ingestion_ts
    from source
)

select * from cleaned
