select
    order_item_id,
    order_id,
    menu_item_id,
    quantity,
    unit_price,
    line_total
from {{ source('bronze', 'order_items') }}
