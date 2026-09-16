-- Fails (returns rows) if any SUCCESS payment's amount disagrees with its order's
-- order_total - the same invariant fact_writer.py's
-- check_payment_amount_matches_order_total already guards in Python.
select
    p.order_id,
    p.amount,
    o.order_total
from {{ ref('stg_payments') }} p
join {{ ref('stg_orders') }} o on p.order_id = o.order_id
where p.status = 'SUCCESS' and p.amount != o.order_total
