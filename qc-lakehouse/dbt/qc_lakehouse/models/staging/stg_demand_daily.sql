select
    day_index,
    order_date,
    weekday,
    dow_multiplier,
    event_multiplier,
    noise,
    total_multiplier,
    orders,
    event_names
from {{ source('bronze', 'demand_daily') }}
