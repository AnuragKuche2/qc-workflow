-- Fails (returns rows) if any DELIVERED/CANCELLED order does not have exactly one ACCEPTED
-- match attempt, or any UNFULFILLED order has one or more - the same invariant
-- fact_writer.py's check_exactly_one_accepted_match_per_matched_order already guards in
-- Python. Runs against stg_match_attempts directly (not int_order_matching), since that
-- model's own join silently assumes this invariant already holds.
with accepted_counts as (
    select order_id, count(*) as accepted_count
    from {{ ref('stg_match_attempts') }}
    where response = 'ACCEPTED'
    group by order_id
)
select
    o.order_id,
    o.order_status,
    coalesce(ac.accepted_count, 0) as accepted_count
from {{ ref('stg_orders') }} o
left join accepted_counts ac on o.order_id = ac.order_id
where
    (o.order_status = 'UNFULFILLED' and coalesce(ac.accepted_count, 0) != 0)
    or (o.order_status != 'UNFULFILLED' and coalesce(ac.accepted_count, 0) != 1)
