with source as (

    select * from {{ source('bank', 'merchants') }}

)

select
    trim(merchant_id)       as merchant_id,
    trim(merchant_name)     as merchant_name,
    trim(mcc)               as mcc,
    upper(trim(country_code)) as country_code,
    _loaded_at
from source
qualify row_number() over (partition by trim(merchant_id) order by _source_file desc, _loaded_at desc, _source_row desc) = 1
