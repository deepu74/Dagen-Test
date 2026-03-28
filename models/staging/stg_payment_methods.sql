{{ config(materialized='view') }}

with source_payment_methods as (
    select
        payment_method_id,
        customer_id,
        method_type,
        details,
        is_default,
        created_at,
        updated_at
    from {{ source('payments_postgres', 'payment_methods') }}
)

select
    payment_method_id,
    customer_id,
    method_type,
    details,
    is_default,
    created_at,
    updated_at,
    current_timestamp() as dbt_load_timestamp
from source_payment_methods