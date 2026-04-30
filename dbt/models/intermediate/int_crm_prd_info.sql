{{ config(
    materialized = 'incremental',
    unique_key   = 'prd_id',
    schema       = 'silver_dw'
) }}

with source as (
    select * from {{ source('bronze_dw', 'crm_prd_info') }}
    where prd_id is not null
      and prd_start_dt is not null

    {% if is_incremental() %}
      and prd_start_dt > (select max(prd_start_dt) from {{ this }})
    {% endif %}
),

deduped as (
    -- keep latest version per prd_id
    select *
    from source
    qualify row_number() over (
        partition by prd_id
        order by prd_start_dt desc
    ) = 1
),

cleaned as (
    select
        prd_id,
        cat_id,
        trim(prd_key)   as prd_key,
        trim(prd_nm)    as prd_nm,
        prd_cost,
        trim(prd_line)  as prd_line,
        prd_start_dt,
        prd_end_dt,
        current_timestamp() as ingestion_ts
    from deduped
)

select * from cleaned

