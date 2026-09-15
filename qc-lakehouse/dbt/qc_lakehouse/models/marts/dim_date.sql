select
    order_date as date_day,
    day_index,
    weekday,
    dow_multiplier,
    event_multiplier,
    dayofweek(order_date) in (1, 7) as is_weekend,
    event_names
from {{ ref('stg_demand_daily') }}
