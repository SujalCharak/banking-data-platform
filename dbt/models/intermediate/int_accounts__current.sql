select
    account_id,
    customer_id,
    iban,
    account_type,
    currency,
    status,
    opened_date,
    closed_date,
    opening_balance,
    updated_at
from {{ ref('stg_bank__accounts') }}
qualify row_number() over (
    partition by account_id
    order by updated_at desc, _loaded_at desc, _source_row desc
) = 1
