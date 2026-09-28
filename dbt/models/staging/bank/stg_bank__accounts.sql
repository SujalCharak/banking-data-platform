with source as (

    select * from {{ source('bank', 'accounts') }}

)

select
    trim(account_id)                                              as account_id,
    trim(customer_id)                                             as customer_id,
    upper(replace(iban, ' ', ''))                                 as iban,
    upper(trim(account_type))                                     as account_type,
    upper(trim(currency))                                         as currency,
    upper(trim(status))                                           as status,
    {{ try_parse_date('opened_date') }}                           as opened_date,
    {{ try_parse_date('closed_date') }}                           as closed_date,
    try_cast(trim(opening_balance) as decimal(18, 2))             as opening_balance,
    {{ try_parse_timestamp('updated_at', ['%Y-%m-%d %H:%M:%S']) }} as updated_at,
    _source_file,
    _source_row,
    _loaded_at
from source
