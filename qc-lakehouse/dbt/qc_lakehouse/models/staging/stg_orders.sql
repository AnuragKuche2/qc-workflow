with cleaned as (
    select
        order_id,
        order_ref,
        customer_id,
        restaurant_id,
        zone_id,
        placed_at,
        order_status,
        subtotal,
        delivery_fee,
        commission_pct,
        commission_amount,
        order_total,
        delivery_notes as delivery_notes_raw,
        case
            when delivery_notes is null then null
            else concat(
                upper(substring(trim(delivery_notes), 1, 1)),
                lower(substring(trim(delivery_notes), 2))
            )
        end as delivery_notes
    from {{ source('bronze', 'orders') }}
)
select
    order_id,
    order_ref,
    customer_id,
    restaurant_id,
    zone_id,
    placed_at,
    order_status,
    subtotal,
    delivery_fee,
    commission_pct,
    commission_amount,
    order_total,
    delivery_notes,
    delivery_notes_raw is not null and delivery_notes_raw != delivery_notes as was_text_noise_defect
from cleaned
