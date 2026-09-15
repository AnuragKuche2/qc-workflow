select
    restaurant_id,
    restaurant_ref,
    name,
    cuisine_type,
    city_id,
    zone_id,
    lat,
    lon,
    commission_pct,
    payout_account_id,
    prep_time_p50_minutes,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'restaurants') }}
