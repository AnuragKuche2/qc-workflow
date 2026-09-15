select
    menu_item_id,
    restaurant_id,
    name,
    category,
    price,
    is_available,
    created_at,
    updated_at
from {{ source('bronze', 'menu_items') }}
