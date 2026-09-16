-- Fails (returns rows) if any order's fct_orders.subtotal disagrees with the sum of its
-- own order_items.line_total - the same invariant fact_writer.py's
-- check_subtotal_matches_line_items already guards in Python, re-verified here over the
-- gold layer to confirm this dbt project's transformations didn't silently corrupt it.
select
    fo.order_id,
    fo.subtotal,
    sum(oi.line_total) as computed_subtotal
from {{ ref('fct_orders') }} fo
join {{ ref('stg_order_items') }} oi on fo.order_id = oi.order_id
group by fo.order_id, fo.subtotal
having fo.subtotal != sum(oi.line_total)
