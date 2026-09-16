with item_totals as (
    select order_id, count(*) as item_count
    from {{ ref('stg_order_items') }}
    group by order_id
),
refund_totals as (
    select order_id, cast(sum(refund_amount) as decimal(18, 2)) as total_refunded
    from {{ ref('stg_refunds') }}
    group by order_id
)
select
    o.order_id,
    o.order_ref,
    o.customer_id,
    o.restaurant_id,
    o.zone_id,
    o.placed_at,
    o.order_status,
    o.subtotal,
    o.delivery_fee,
    o.commission_pct,
    o.commission_amount,
    o.order_total,
    cast(coalesce(r.total_refunded, 0) as decimal(18, 2)) as refund_amount,
    cast(o.order_total - coalesce(r.total_refunded, 0) as decimal(18, 2)) as net_revenue,
    coalesce(i.item_count, 0) as item_count
from {{ ref('stg_orders') }} o
left join item_totals i on o.order_id = i.order_id
left join refund_totals r on o.order_id = r.order_id
