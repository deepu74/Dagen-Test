{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
payment_methods as (
    select * from {{ ref('stg_payment_methods') }}
)

select
    t.transaction_id,
    t.amount,
    t.currency,
    t.status,
    t.reference,
    t.created_at,
    t.updated_at,
    c.customer_id as debtor_customer_id,
    c.customer_type,
    c.kyc_status,
    c.risk_profile,
    pm.method_type,
    pm.is_default as payment_method_is_default,
    t.dbt_load_timestamp
from transactions t
left join customers c on t.debtor_customer_id = c.customer_id
left join payment_methods pm on t.payment_method_id = pm.payment_method_id