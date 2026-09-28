with balances as (

    select
        a.customer_id,
        b.balance_date,
        cast({{ dbt.date_trunc('month', 'b.balance_date') }} as date) as month_start,
        sum(b.closing_balance_eur) as customer_balance_eur
    from {{ ref('fct_account_daily_balance') }} b
    inner join {{ ref('dim_account') }} a
        on a.account_id = b.account_id
    group by a.customer_id, b.balance_date

),

monthly_balances as (

    select
        customer_id,
        month_start,
        max(balance_date)                   as last_balance_date,
        avg(customer_balance_eur)           as avg_daily_balance_eur
    from balances
    group by customer_id, month_start

),

month_end_balances as (

    select m.customer_id, m.month_start, m.avg_daily_balance_eur, b.customer_balance_eur as month_end_balance_eur
    from monthly_balances m
    inner join balances b
        on b.customer_id = m.customer_id
       and b.balance_date = m.last_balance_date

),

reversed as (

    select distinct original_txn_id
    from {{ ref('fct_transactions') }}
    where txn_type = 'REVERSAL' and is_posted

),

activity as (

    select
        t.customer_id,
        cast({{ dbt.date_trunc('month', 't.txn_date') }} as date) as month_start,
        count(*) as posted_txn_count,
        sum(case when t.txn_category = 'INCOME' then t.amount_eur else 0 end) as income_eur,
        sum(case
                when t.txn_type = 'CARD_PAYMENT' and r.original_txn_id is null then t.amount_eur
                else 0
            end) as card_spend_eur,
        sum(case
                when t.direction = 'CR' and t.txn_type not in ('INTERNAL_TRANSFER', 'REVERSAL') then t.amount_eur
                else 0
            end) as external_inflow_eur,
        sum(case
                when t.direction = 'DR' and t.txn_type <> 'INTERNAL_TRANSFER' and r.original_txn_id is null
                then t.amount_eur
                else 0
            end) as external_outflow_eur,
        count(distinct t.merchant_id) as distinct_merchants
    from {{ ref('fct_transactions') }} t
    left join reversed r
        on r.original_txn_id = t.txn_id
    where t.is_posted
      and t.txn_type <> 'REVERSAL'
    group by t.customer_id, cast({{ dbt.date_trunc('month', 't.txn_date') }} as date)

),

combined as (

    select
        b.customer_id,
        b.month_start,
        coalesce(a.posted_txn_count, 0)     as posted_txn_count,
        coalesce(a.income_eur, 0)           as income_eur,
        coalesce(a.card_spend_eur, 0)       as card_spend_eur,
        coalesce(a.external_inflow_eur, 0)  as external_inflow_eur,
        coalesce(a.external_outflow_eur, 0) as external_outflow_eur,
        coalesce(a.distinct_merchants, 0)   as distinct_merchants,
        cast(b.avg_daily_balance_eur as decimal(18, 2)) as avg_daily_balance_eur,
        b.month_end_balance_eur
    from month_end_balances b
    left join activity a
        on a.customer_id = b.customer_id
       and a.month_start = b.month_start

),

with_profile as (

    -- segment and risk rating as they were at the end of each month
    select
        c.*,
        d.segment,
        d.risk_rating
    from combined c
    left join {{ ref('dim_customer') }} d
        on d.customer_id = c.customer_id
       and d.valid_from < cast({{ dbt.dateadd('month', 1, 'c.month_start') }} as timestamp)
       and (d.valid_to is null or d.valid_to >= cast({{ dbt.dateadd('month', 1, 'c.month_start') }} as timestamp))

)

select
    customer_id,
    month_start,
    segment,
    risk_rating,
    posted_txn_count,
    income_eur,
    card_spend_eur,
    external_inflow_eur,
    external_outflow_eur,
    external_inflow_eur - external_outflow_eur as net_external_flow_eur,
    distinct_merchants,
    avg_daily_balance_eur,
    month_end_balance_eur,
    lag(card_spend_eur) over (partition by customer_id order by month_start) as prior_month_card_spend_eur,
    percent_rank() over (partition by month_start, segment order by card_spend_eur) as card_spend_percentile_in_segment
from with_profile
