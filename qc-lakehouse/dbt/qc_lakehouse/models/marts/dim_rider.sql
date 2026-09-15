select
    rider_id,
    rider_ref,
    vehicle_type,
    payout_tier,
    home_zone_id,
    is_active
from {{ ref('stg_riders') }}
