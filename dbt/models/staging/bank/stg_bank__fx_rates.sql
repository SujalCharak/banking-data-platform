with raw_rates as (

    select * from {{ source('bank', 'fx_rates') }}

)

select
    {{ try_parse_date('raw_rates.rate_date') }}             as rate_date,
    upper(trim(raw_rates.currency))                         as currency,
    try_cast(trim(raw_rates.rate_to_eur) as decimal(18, 8)) as rate_to_eur,
    trim(raw_rates.source)                                  as rate_source,
    raw_rates._loaded_at
from raw_rates
qualify row_number() over (
    partition by raw_rates.rate_date, upper(trim(raw_rates.currency))
    order by raw_rates._loaded_at desc, raw_rates._source_row desc
) = 1
