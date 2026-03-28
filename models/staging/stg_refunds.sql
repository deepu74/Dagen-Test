{{ config(materialized='view') }}

with source_refunds as (
    select
        refund_id,
        original_transaction_id,
        amount,
        currency,
        reason,
        status,
        created_at,
        updated_at
    from {{ source('payments_postgres', 'refunds') }}
)

select
    refund_id,
    original_transaction_id,
    amount,
    currency,
    reason,
    status,
    created_at,
    updated_at,
    current_timestamp() as dbt_load_timestamp
from source_refunds