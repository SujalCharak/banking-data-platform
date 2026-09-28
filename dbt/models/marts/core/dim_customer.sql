{{ config(post_hook="{{ bank_platform.apply_masking_policy('email', 'MASK_EMAIL') }}") }}

select
    customer_version_key,
    customer_id,
    first_name,
    last_name,
    email,
    birth_date,
    country_code,
    segment,
    risk_rating,
    kyc_status,
    created_at,
    valid_from,
    valid_to,
    valid_to is null as is_current
from {{ ref('int_customers__history') }}
