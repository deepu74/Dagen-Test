{{ config(materialized='table') }}

with refunds_enriched as (
    select * from {{ ref('int_refunds_with_transaction') }}
)

select
    refund_id,
    original_transaction_id,
    refund_amount,
    refund_currency,
    reason,
    refund_status,
    original_transaction_amount,
    original_transaction_currency,
    original_transaction_status,
    refund_percentage,
    refund_created_at,
    refund_updated_at,
    original_transaction_created_at,
    dbt_load_timestamp,
    -- Add derived metrics
    case
        when refund_status = 'completed' then refund_amount
        else 0
    end as completed_refund_amount,
    case
        when refund_status = 'failed' then 1
        else 0
    end as is_refund_failed,
    datediff(day, original_transaction_created_at, refund_created_at) as days_to_refund
from refunds_enriched