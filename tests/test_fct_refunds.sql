-- Test for duplicate refund IDs
select
    refund_id,
    count(*) as count
from {{ ref('fct_refunds') }}
group by refund_id
having count(*) > 1