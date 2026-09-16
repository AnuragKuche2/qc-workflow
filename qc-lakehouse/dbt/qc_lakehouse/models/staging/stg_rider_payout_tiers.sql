select
    tier,
    valid_from,
    valid_to,
    base_fare,
    per_km_rate,
    per_minute_rate
from {{ source('bronze', 'rider_payout_tiers') }}
