with attempt_counts as (
    select order_id, count(*) as total_attempts
    from {{ ref('stg_match_attempts') }}
    group by order_id
),
accepted as (
    select
        order_id,
        match_id as accepted_match_id,
        rider_id as accepted_rider_id,
        offered_at as accepted_offered_at,
        responded_at as accepted_at
    from {{ ref('stg_match_attempts') }}
    where response = 'ACCEPTED'
)
select
    o.order_id,
    o.placed_at,
    a.accepted_match_id,
    a.accepted_rider_id,
    a.accepted_offered_at,
    a.accepted_at,
    coalesce(c.total_attempts, 0) as total_attempts,
    a.accepted_match_id is not null as was_matched
from {{ ref('stg_orders') }} o
left join attempt_counts c on o.order_id = c.order_id
left join accepted a on o.order_id = a.order_id
