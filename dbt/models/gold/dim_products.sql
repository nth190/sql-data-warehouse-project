{{ config(
    materialized = 'incremental',
    unique_key = 'product_key',
    schema = 'gold_dw',
    tags = ['gold', 'dimension', 'scd2']
) }}

with source_data as (
    select * from {{ ref('int_products_enriched') }}
),

{% if is_incremental() %}

-- Get existing dimension records
existing_dim as (
    select * from {{ this }}
),

-- Identify new and changed records
source_with_hash as (
    select
        prd_id as product_id,
        prd_key as product_number,
        prd_nm as product_name,
        cat as category_code,
        subcat as category_name,
        maintenance as parent_category,
        prd_start_dt as create_date,
        MD5(CONCAT(
            COALESCE(prd_nm, ''),
            COALESCE(cat, ''),
            COALESCE(subcat, '')
        )) as row_hash,
        ingestion_ts
    from source_data
),

existing_with_hash as (
    select
        product_key,
        product_id,
        product_number,
        product_name,
        category_code,
        category_name,
        parent_category,
        create_date,
        MD5(CONCAT(
            COALESCE(product_name, ''),
            COALESCE(category_code, ''),
            COALESCE(category_name, '')
        )) as row_hash,
        valid_from,
        valid_to,
        is_current
    from existing_dim
),

-- Identify changed records (close out old versions)
records_to_close as (
    select
        e.product_key,
        e.product_id,
        e.product_number,
        e.product_name,
        e.category_code,
        e.category_name,
        e.parent_category,
        e.create_date,
        e.valid_from,
        current_timestamp() as valid_to,
        false as is_current
    from existing_with_hash e
    inner join source_with_hash s
        on e.product_id = s.product_id
        and e.is_current = true
        and e.row_hash != s.row_hash
),

-- New versions of changed records
new_versions as (
    select
        MD5(CONCAT(
            COALESCE(CAST(s.product_id AS CHAR), ''),
            COALESCE(CAST(s.ingestion_ts AS CHAR), '')
        )) as product_key,
        s.product_id,
        s.product_number,
        s.product_name,
        s.category_code,
        s.category_name,
        s.parent_category,
        s.create_date,
        current_timestamp() as valid_from,
        cast('9999-12-31' as datetime) as valid_to,
        true as is_current
    from source_with_hash s
    inner join existing_with_hash e
        on s.product_id = e.product_id
        and e.is_current = true
        and s.row_hash != e.row_hash
),

-- Completely new records
new_records as (
    select
        MD5(CONCAT(
            COALESCE(CAST(s.product_id AS CHAR), ''),
            COALESCE(CAST(s.ingestion_ts AS CHAR), '')
        )) as product_key,
        s.product_id,
        s.product_number,
        s.product_name,
        s.category_code,
        s.category_name,
        s.parent_category,
        s.create_date,
        current_timestamp() as valid_from,
        cast('9999-12-31' as datetime) as valid_to,
        true as is_current
    from source_with_hash s
    left join existing_with_hash e
        on s.product_id = e.product_id
    where e.product_id is null
),

-- Unchanged records
unchanged_records as (
    select
        e.product_key,
        e.product_id,
        e.product_number,
        e.product_name,
        e.category_code,
        e.category_name,
        e.parent_category,
        e.create_date,
        e.valid_from,
        e.valid_to,
        e.is_current
    from existing_with_hash e
    inner join source_with_hash s
        on e.product_id = s.product_id
        and e.is_current = true
        and e.row_hash = s.row_hash
    
    union all
    
    select
        product_key,
        product_id,
        product_number,
        product_name,
        category_code,
        category_name,
        parent_category,
        create_date,
        valid_from,
        valid_to,
        is_current
    from existing_with_hash
    where is_current = false
),

final as (
    select * from records_to_close
    union all
    select * from new_versions
    union all
    select * from new_records
    union all
    select * from unchanged_records
)

select * from final

{% else %}

-- Initial load
initial_load as (
    select
        MD5(CONCAT(
            COALESCE(CAST(prd_id AS CHAR), ''),
            COALESCE(CAST(ingestion_ts AS CHAR), '')
        )) as product_key,
        prd_id as product_id,
        prd_key as product_number,
        prd_nm as product_name,
        cat as category_code,
        subcat as category_name,
        maintenance as parent_category,
        prd_start_dt as create_date,
        cast(prd_start_dt as datetime) as valid_from,
        cast('9999-12-31' as datetime) as valid_to,
        true as is_current
    from source_data
)

select * from initial_load

{% endif %}
