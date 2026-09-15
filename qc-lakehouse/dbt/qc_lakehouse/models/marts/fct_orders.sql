select
    order_id,
    order_ref,
    customer_id,
    restaurant_id,
    zone_id,
    date(placed_at) as date_day,
    placed_at,
    order_status,
    subtotal,
    delivery_fee,
    commission_pct,
    commission_amount,
    order_total,
    refund_amount,
    net_revenue,
    item_count
from {{ ref('int_order_economics') }}
