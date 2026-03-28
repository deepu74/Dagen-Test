{{ config(materialized='table') }}

with transactions_enriched as (
    select * from {{ ref('int_transactions_with_customer') }}
)

select
    transaction_id,
    debtor_customer_id,
    customer_type,
    kyc_status,
    risk_profile,
    amount,
    currency,
    status,
    method_type,
    payment_method_is_default,
    reference,
    created_at,
    updated_at,
    dbt_load_timestamp,
    -- Add derived metrics
    case
        when status = 'completed' then amount
        else 0
    end as completed_amount,
    case
        when status = 'failed' then 1
        else 0
    end as is_failed,
    case
        when status = 'pending' then 1
        else 0
    end as is_pending
from transactions_enriched