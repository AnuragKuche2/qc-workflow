select
    customer_id,
    customer_ref,
    sha2(lower(trim(email)), 256) as email_hash,
    concat(
        substring(phone, 1, 3),
        repeat('*', length(phone) - 6),
        substring(phone, length(phone) - 2, 3)
    ) as phone_masked,
    full_name,
    city_id,
    home_zone_id,
    lat,
    lon,
    signup_ts,
    created_at,
    updated_at
from {{ source('bronze', 'customers') }}
