with rates as (

    select rate_date, currency, rate_to_eur
    from {{ ref('stg_bank__fx_rates') }}
    where currency <> 'EUR'
      and rate_to_eur is not null

),

bounds as (

    select min(rate_date) as first_date, max(rate_date) as last_date
    from rates

),

calendar as (

    select cast({{ dbt.dateadd('day', 'n.n', 'b.first_date') }} as date) as rate_date
    from bounds b
    cross join ({{ integer_series() }}) n
    where n.n <= {{ dbt.datediff('b.first_date', 'b.last_date', 'day') }} + {{ var('fx_forward_fill_days') }}

),

grid as (

    select c.rate_date, cur.currency
    from calendar c
    cross join (select distinct currency from rates) cur

),

joined as (

    select
        g.rate_date,
        g.currency,
        r.rate_to_eur,
        r.rate_date as published_date
    from grid g
    left join rates r
        on r.currency = g.currency
       and r.rate_date = g.rate_date

),

grouped as (

    -- every published rate opens a new group; unpublished days fall into the group before them
    select
        *,
        count(published_date) over (
            partition by currency order by rate_date
            rows between unbounded preceding and current row
        ) as fill_group
    from joined

),

filled as (

    select
        rate_date,
        currency,
        max(rate_to_eur) over (partition by currency, fill_group)    as rate_to_eur,
        max(published_date) over (partition by currency, fill_group) as published_date
    from grouped

)

select
    rate_date,
    currency,
    rate_to_eur,
    published_date,
    rate_date <> published_date as is_carried_forward
from filled

union all

select
    rate_date,
    'EUR',
    cast(1 as decimal(18, 8)),
    rate_date,
    false
from calendar
