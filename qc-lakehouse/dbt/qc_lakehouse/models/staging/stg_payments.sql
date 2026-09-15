select
    payment_id,
    order_id,
    amount,
    method,
    status,
    paid_at
from {{ source('bronze', 'payments') }}
