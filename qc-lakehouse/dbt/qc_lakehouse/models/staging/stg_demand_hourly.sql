select
    day_index,
    order_date,
    hour,
    orders
from {{ source('bronze', 'demand_hourly') }}
