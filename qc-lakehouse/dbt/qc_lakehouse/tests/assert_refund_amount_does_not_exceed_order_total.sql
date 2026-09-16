-- Fails (returns rows) if any refund's amount exceeds its order's order_total - a refund
-- should never return more than what was actually charged. Currently holds only because
-- build_refunds bounds refund_fraction to [0.3, 1.0] by construction
-- (src/qc_lakehouse/generator/fact_entities.py) - this test guards against that invariant
-- silently breaking on a future generator change, matching the same independent
-- re-verification pattern as this directory's other 3 singular tests.
select
    r.refund_id,
    r.order_id,
    r.refund_amount,
    o.order_total
from {{ ref('stg_refunds') }} r
join {{ ref('stg_orders') }} o on r.order_id = o.order_id
where r.refund_amount > o.order_total
