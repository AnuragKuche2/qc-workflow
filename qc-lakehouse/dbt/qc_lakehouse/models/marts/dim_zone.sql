select
    z.zone_id,
    z.name as zone_name,
    z.center_lat,
    z.center_lon,
    z.ring,
    z.base_delivery_fee,
    z.sla_target_minutes,
    z.is_active,
    c.city_id,
    c.name as city_name,
    c.country_code,
    c.timezone
from {{ ref('stg_zones') }} z
join {{ ref('stg_cities') }} c on z.city_id = c.city_id
