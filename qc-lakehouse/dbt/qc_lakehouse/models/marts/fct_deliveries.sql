select
    om.order_id,
    o.customer_id,
    o.restaurant_id,
    o.zone_id,
    date(om.placed_at) as date_day,
    om.placed_at,
    om.accepted_rider_id,
    om.total_attempts,
    om.was_matched,
    om.accepted_at,
    case
        when om.accepted_at is not null
        then unix_timestamp(om.accepted_at) - unix_timestamp(om.placed_at)
    end as seconds_to_accept
from {{ ref('int_order_matching') }} om
join {{ ref('stg_orders') }} o on om.order_id = o.order_id
