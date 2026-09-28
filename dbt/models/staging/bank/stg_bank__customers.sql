with source as (

    select * from {{ source('bank', 'customers') }}

)

select
    trim(customer_id)                                         as customer_id,
    trim(first_name)                                          as first_name,
    trim(last_name)                                           as last_name,
    lower(trim(email))                                        as email,
    {{ try_parse_date('birth_date') }}                        as birth_date,
    upper(trim(country_code))                                 as country_code,
    upper(trim(segment))                                      as segment,
    upper(trim(risk_rating))                                  as risk_rating,
    upper(trim(kyc_status))                                   as kyc_status,
    {{ try_parse_timestamp('created_at', ['%Y-%m-%d %H:%M:%S']) }} as created_at,
    {{ try_parse_timestamp('updated_at', ['%Y-%m-%d %H:%M:%S']) }} as updated_at,
    _source_file,
    _source_row,
    _loaded_at
from source
