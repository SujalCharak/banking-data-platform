{{ config(post_hook="{{ bank_platform.apply_masking_policy('iban', 'MASK_IBAN') }}") }}

select
    account_id,
    customer_id,
    iban,
    account_type,
    currency,
    status,
    status = 'ACTIVE' as is_active,
    opened_date,
    closed_date,
    opening_balance,
    updated_at
from {{ ref('int_accounts__current') }}
