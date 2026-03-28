-- Test for duplicate transaction IDs
select
    transaction_id,
    count(*) as count
from {{ ref('fct_transactions') }}
group by transaction_id
having count(*) > 1