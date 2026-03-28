-- Test for duplicate customer IDs
select
    customer_id,
    count(*) as count
from {{ ref('dim_customers') }}
group by customer_id
having count(*) > 1