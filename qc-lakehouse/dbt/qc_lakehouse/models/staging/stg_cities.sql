select
    city_id,
    name,
    country_code,
    timezone,
    lat,
    lon,
    created_at,
    updated_at
from {{ source('bronze', 'cities') }}
