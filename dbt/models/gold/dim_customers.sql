{{ config(
    materialized = 'incremental',
    unique_key = 'customer_key',
    schema = 'gold_dw',
    tags = ['gold', 'dimension', 'scd2']
) }}

with source_data as (
    select * from {{ ref('int_customers_enriched') }}
),

{% if is_incremental() %}

-- Get existing dimension records
existing_dim as (
    select * from {{ this }}
),

-- Identify new and changed records
source_with_hash as (
    select
        cst_id as customer_id,
        cst_key as customer_number,
        cst_firstname as first_name,
        cst_lastname as last_name,
        country_code,
        country_name,
        region,
        segment,
        cst_create_date as create_date,
        MD5(CONCAT(
            COALESCE(cst_firstname, ''),
            COALESCE(cst_lastname, ''),
            COALESCE(country_code, ''),
            COALESCE(segment, '')
        )) as row_hash,
        ingestion_ts
    from source_data
),

existing_with_hash as (
    select
        customer_key,
        customer_id,
        customer_number,
        first_name,
        last_name,
        country_code,
        country_name,
        region,
        segment,
        create_date,
        MD5(CONCAT(
            COALESCE(first_name, ''),
            COALESCE(last_name, ''),
            COALESCE(country_code, ''),
            COALESCE(segment, '')
        )) as row_hash,
        valid_from,
        valid_to,
        is_current
    from existing_dim
),

-- Identify changed records (close out old versions)
records_to_close as (
    select
        e.customer_key,
        e.customer_id,
        e.customer_number,
        e.first_name,
        e.last_name,
        e.country_code,
        e.country_name,
        e.region,
        e.segment,
        e.create_date,
        e.valid_from,
        current_timestamp() as valid_to,  -- Close the record
        false as is_current
    from existing_with_hash e
    inner join source_with_hash s
        on e.customer_id = s.customer_id
        and e.is_current = true
        and e.row_hash != s.row_hash  -- Data has changed
),

-- New versions of changed records
new_versions as (
    select
        MD5(CONCAT(
            COALESCE(CAST(s.customer_id AS CHAR), ''),
            COALESCE(CAST(s.ingestion_ts AS CHAR), '')
        )) as customer_key,
        s.customer_id,
        s.customer_number,
        s.first_name,
        s.last_name,
        s.country_code,
        s.country_name,
        s.region,
        s.segment,
        s.create_date,
        current_timestamp() as valid_from,
        cast('9999-12-31' as datetime) as valid_to,
        true as is_current
    from source_with_hash s
    inner join existing_with_hash e
        on s.customer_id = e.customer_id
        and e.is_current = true
        and s.row_hash != e.row_hash  -- Data has changed
),

-- Completely new records
new_records as (
    select
        MD5(CONCAT(
            COALESCE(CAST(s.customer_id AS CHAR), ''),
            COALESCE(CAST(s.ingestion_ts AS CHAR), '')
        )) as customer_key,
        s.customer_id,
        s.customer_number,
        s.first_name,
        s.last_name,
        s.country_code,
        s.country_name,
        s.region,
        s.segment,
        s.create_date,
        current_timestamp() as valid_from,
        cast('9999-12-31' as datetime) as valid_to,
        true as is_current
    from source_with_hash s
    left join existing_with_hash e
        on s.customer_id = e.customer_id
    where e.customer_id is null  -- Not in dimension yet
),

-- Unchanged records (keep as is)
unchanged_records as (
    select
        e.customer_key,
        e.customer_id,
        e.customer_number,
        e.first_name,
        e.last_name,
        e.country_code,
        e.country_name,
        e.region,
        e.segment,
        e.create_date,
        e.valid_from,
        e.valid_to,
        e.is_current
    from existing_with_hash e
    inner join source_with_hash s
        on e.customer_id = s.customer_id
        and e.is_current = true
        and e.row_hash = s.row_hash  -- No change
    
    union all
    
    -- Keep historical records
    select
        customer_key,
        customer_id,
        customer_number,
        first_name,
        last_name,
        country_code,
        country_name,
        region,
        segment,
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

-- Initial load - all records are current
initial_load as (
    select
        MD5(CONCAT(
            COALESCE(CAST(cst_id AS CHAR), ''),
            COALESCE(CAST(ingestion_ts AS CHAR), '')
        )) as customer_key,
        cst_id as customer_id,
        cst_key as customer_number,
        cst_firstname as first_name,
        cst_lastname as last_name,
        country_code,
        country_name,
        region,
        segment,
        cst_create_date as create_date,
        cast(cst_create_date as datetime) as valid_from,
        cast('9999-12-31' as datetime) as valid_to,
        true as is_current
    from source_data
)

select * from initial_load

{% endif %}
