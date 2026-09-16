select
    rider_id,
    rider_ref,
    vehicle_type,
    payout_tier,
    home_zone_id,
    payout_account_id,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'riders') }}
