select
    match_id,
    order_id,
    rider_id,
    attempt_number,
    offered_at,
    response,
    responded_at
from {{ source('bronze', 'match_attempts') }}
