select
    refund_id,
    order_id,
    payment_id,
    cast(regexp_replace(refund_amount_raw, '[()]', '') as decimal(18, 2)) as refund_amount,
    refund_amount_raw like '(%' as was_money_text_defect,
    reason,
    refunded_at
from {{ source('bronze', 'refunds') }}
