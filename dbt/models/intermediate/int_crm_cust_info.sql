{{ config(
    materialized = 'incremental',
    unique_key   = 'cst_id',
    schema       = 'silver_dw'
) }}

with source as (
    select * from {{ source('bronze_dw', 'crm_cust_info') }}
    where cst_id is not null
      and cst_id <> 0
      and cst_create_date is not null

    {% if is_incremental() %}
      and cst_create_date > (select max(cst_create_date) from {{ this }})
    {% endif %}
),

deduped as (
    -- keep latest version per cst_id (same as ROW_NUMBER() in MySQL)
    select *
    from source
    qualify row_number() over (
        partition by cst_id
        order by cst_create_date desc
    ) = 1
),

cleaned as (
    select
        cst_id,
        cst_key,
        trim(cst_firstname)   as cst_firstname,
        trim(cst_lastname)    as cst_lastname,
        case
            when upper(trim(cst_marital_status)) = 'S' then 'Single'
            when upper(trim(cst_marital_status)) = 'M' then 'Married'
            else 'n/a'
        end                   as cst_marital_status,
        case
            when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'n/a'
        end                   as cst_gndr,
        cst_create_date,
        current_timestamp()   as ingestion_ts
    from deduped
)

select * from cleaned
