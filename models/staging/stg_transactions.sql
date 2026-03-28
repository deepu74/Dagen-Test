{{ config(materialized='view') }}

with source_transactions as (
    select
        transaction_id,
        amount,
        currency,
        status,
        payment_method_id,
        debtor_customer_id,
        creditor_customer_id,
        reference,
        created_at,
        updated_at
    from {{ source('payments_postgres', 'transactions') }}
)

select
    transaction_id,
    amount,
    currency,
    status,
    payment_method_id,
    debtor_customer_id,
    creditor_customer_id,
    reference,
    created_at,
    updated_at,
    current_timestamp() as dbt_load_timestamp
from source_transactions