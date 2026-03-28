{{ config(materialized='view') }}

with source_customers as (
    select
        customer_id,
        customer_type,
        email,
        phone_number,
        address,
        kyc_status,
        risk_profile,
        created_at,
        updated_at
    from {{ source('payments_postgres', 'customers') }}
)

select
    customer_id,
    customer_type,
    email,
    phone_number,
    address,
    kyc_status,
    risk_profile,
    created_at,
    updated_at,
    current_timestamp() as dbt_load_timestamp
from source_customers