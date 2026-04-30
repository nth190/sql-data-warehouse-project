-- Assert that SCD2 valid_from/valid_to dates don't overlap for same customer

with customer_versions as (
    select
        customer_id,
        customer_key,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by customer_id order by valid_from) as next_valid_from
    from {{ ref('dim_customers') }}
)

select
    customer_id,
    customer_key,
    valid_from,
    valid_to,
    next_valid_from,
    'Overlapping date ranges' as error_message
from customer_versions
where next_valid_from is not null
    and valid_to != next_valid_from
