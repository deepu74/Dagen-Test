{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
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
    dbt_load_timestamp,
    -- Add derived fields
    case
        when kyc_status = 'verified' then true
        else false
    end as is_kyc_verified,
    case
        when risk_profile = 'low' then 1
        when risk_profile = 'medium' then 2
        when risk_profile = 'high' then 3
        else 0
    end as risk_score
from customers