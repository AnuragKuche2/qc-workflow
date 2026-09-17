-- Fails (returns rows) if any (day_index, hour) pair appears more than once in the bronze
-- demand_hourly source - build_orders_shell (src/qc_lakehouse/generator/fact_entities.py)
-- joins orders to demand_hourly on this exact composite key to derive each order's
-- contiguous order_id, so a duplicate pair would silently double-count or misassign orders.
-- No single column is unique alone (see _staging__sources.yml), hence a singular test rather
-- than a generic `unique` column test.
select
    day_index,
    hour,
    count(*) as row_count
from {{ source('bronze', 'demand_hourly') }}
group by day_index, hour
having count(*) > 1
