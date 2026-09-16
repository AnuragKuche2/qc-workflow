select
    restaurant_id,
    restaurant_ref,
    name,
    cuisine_type,
    city_id,
    zone_id,
    commission_pct,
    prep_time_p50_minutes,
    is_active
from {{ ref('stg_restaurants') }}
