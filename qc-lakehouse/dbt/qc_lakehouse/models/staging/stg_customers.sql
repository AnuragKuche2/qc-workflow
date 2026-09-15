select
    customer_id,
    customer_ref,
    sha2(concat('{{ env_var("PII_HASH_SALT") }}', lower(trim(email))), 256) as email_hash,
    concat(
        substring(phone, 1, 3),
        repeat('*', length(phone) - 6),
        substring(phone, length(phone) - 2, 3)
    ) as phone_masked,
    -- full_name/lat/lon pass through unmasked by design - only email/phone are in scope for
    -- masking here (see the Sub-project C design spec). full_name and precise coordinates
    -- reach dim_customer in gold; treat this as the project's current PII boundary, not an
    -- oversight, if extending masking scope later.
    full_name,
    city_id,
    home_zone_id,
    lat,
    lon,
    signup_ts,
    created_at,
    updated_at
from {{ source('bronze', 'customers') }}
