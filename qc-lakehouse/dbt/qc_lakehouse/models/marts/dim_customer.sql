select
    customer_id,
    customer_ref,
    email_hash,
    phone_masked,
    full_name,
    city_id,
    home_zone_id,
    lat,
    lon,
    signup_ts
from {{ ref('stg_customers') }}
