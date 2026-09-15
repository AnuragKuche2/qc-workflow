select
    zone_id,
    city_id,
    name,
    center_lat,
    center_lon,
    ring,
    base_delivery_fee,
    sla_target_minutes,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'zones') }}
