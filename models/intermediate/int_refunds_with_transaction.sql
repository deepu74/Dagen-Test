{{ config(materialized='table') }}

with refunds as (
    select * from {{ ref('stg_refunds') }}
),
transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    r.refund_id,
    r.original_transaction_id,
    r.amount as refund_amount,
    r.currency as refund_currency,
    r.reason,
    r.status as refund_status,
    r.created_at as refund_created_at,
    r.updated_at as refund_updated_at,
    t.amount as original_transaction_amount,
    t.currency as original_transaction_currency,
    t.status as original_transaction_status,
    t.created_at as original_transaction_created_at,
    (r.amount / t.amount * 100)::numeric(10, 2) as refund_percentage,
    r.dbt_load_timestamp
from refunds r
left join transactions t on r.original_transaction_id = t.transaction_id