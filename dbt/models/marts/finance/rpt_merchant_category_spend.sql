with reversed as (

    select distinct original_txn_id
    from {{ ref('fct_transactions') }}
    where txn_type = 'REVERSAL' and is_posted

),

card_spend as (

    select
        cast({{ dbt.date_trunc('month', 't.txn_date') }} as date) as month_start,
        coalesce(m.spend_category, 'Unknown')                     as spend_category,
        coalesce(m.country_code, 'UNKNOWN')                       as merchant_country,
        t.customer_id,
        t.amount_eur
    from {{ ref('fct_transactions') }} t
    left join reversed r
        on r.original_txn_id = t.txn_id
    left join {{ ref('dim_merchant') }} m
        on m.merchant_id = t.merchant_id
    where t.txn_type = 'CARD_PAYMENT'
      and t.is_posted
      and r.original_txn_id is null

),

monthly as (

    select
        month_start,
        spend_category,
        merchant_country,
        count(*)                    as txn_count,
        count(distinct customer_id) as active_customers,
        sum(amount_eur)             as spend_eur,
        avg(amount_eur)             as avg_ticket_eur
    from card_spend
    group by month_start, spend_category, merchant_country

),

with_prior as (

    select
        *,
        lag(spend_eur) over (partition by spend_category, merchant_country order by month_start) as prior_month_spend_eur,
        sum(spend_eur) over (partition by month_start)                                           as month_total_spend_eur
    from monthly

)

select
    month_start,
    spend_category,
    merchant_country,
    txn_count,
    active_customers,
    spend_eur,
    cast(avg_ticket_eur as decimal(18, 2))                                         as avg_ticket_eur,
    cast(spend_eur / nullif(month_total_spend_eur, 0) as decimal(9, 6))            as share_of_month_spend,
    prior_month_spend_eur,
    cast((spend_eur - prior_month_spend_eur) / nullif(prior_month_spend_eur, 0) as decimal(12, 6)) as mom_growth
from with_prior
