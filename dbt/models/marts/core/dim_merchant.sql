select
    m.merchant_id,
    m.merchant_name,
    m.mcc,
    coalesce(c.mcc_description, 'Unknown')  as mcc_description,
    coalesce(c.spend_category, 'Other')     as spend_category,
    m.country_code
from {{ ref('stg_bank__merchants') }} m
left join {{ ref('mcc_codes') }} c
    on c.mcc = m.mcc
