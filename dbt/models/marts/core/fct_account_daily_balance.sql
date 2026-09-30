{%- set as_iceberg = target.type == 'snowflake' and var('enable_iceberg') -%}

{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'balance_date'],
        incremental_strategy=merge_strategy(),
        on_schema_change='fail',
        cluster_by=['balance_date']
    )
}}

{%- if as_iceberg %}
{{ config(table_format='iceberg', external_volume='SNOWFLAKE_MANAGED') }}
{%- endif %}

{#-
    Balances are a running sum, so a late transaction changes every balance after it.
    On incremental runs we find the earliest transaction date among newly loaded rows,
    take each account's balance from the day before, and rebuild forward from there.
-#}

{%- set recompute_from = none -%}
{%- if is_incremental() and execute -%}
    {%- set query -%}
        select cast(min(txn_date) as {{ dbt.type_string() }})
        from {{ ref('fct_transactions') }}
        where _loaded_at > (select max(source_loaded_through) from {{ this }})
    {%- endset -%}
    {%- set earliest = run_query(query).columns[0].values()[0] -%}
    {#- adapters return this as text or as a date depending on type inference, so normalise it -#}
    {%- set recompute_from = (earliest | string)[:10] if earliest else '9999-12-31' -%}
    {%- set prior_day = (modules.datetime.date.fromisoformat(recompute_from) - modules.datetime.timedelta(days=1)).isoformat() -%}
    {{ log("fct_account_daily_balance: recomputing from " ~ recompute_from, info=true) }}
{%- endif %}

with bounds as (

    select min(txn_date) as data_start, max(txn_date) as data_end
    from {{ ref('fct_transactions') }}

),

accounts as (

    select
        a.account_id,
        a.currency,
        a.opening_balance,
        case when a.opened_date > b.data_start then a.opened_date else b.data_start end as start_date,
        case
            when a.closed_date is not null and a.closed_date < b.data_end then a.closed_date
            else b.data_end
        end as end_date
    from {{ ref('dim_account') }} a
    cross join bounds b

),

spine as (

    select
        a.account_id,
        a.currency,
        a.opening_balance,
        d.date_day as balance_date
    from accounts a
    inner join {{ ref('dim_date') }} d
        on d.date_day between a.start_date and a.end_date
    {% if recompute_from %}
    where d.date_day >= cast('{{ recompute_from }}' as date)
    {% endif %}

),

daily_activity as (

    select
        account_id,
        txn_date,
        sum(signed_amount)                                          as net_amount,
        sum(case when direction = 'CR' then amount else 0 end)      as credit_amount,
        sum(case when direction = 'DR' then amount else 0 end)      as debit_amount,
        count(*)                                                    as posted_txn_count
    from {{ ref('fct_transactions') }}
    where is_posted
    {% if recompute_from %}
      and txn_date >= cast('{{ recompute_from }}' as date)
    {% endif %}
    group by account_id, txn_date

),

prior_balance as (

    {% if recompute_from %}
    select account_id, closing_balance
    from {{ this }}
    where balance_date = cast('{{ prior_day }}' as date)
    {% else %}
    select cast(null as {{ dbt.type_string() }}) as account_id, cast(null as decimal(18, 2)) as closing_balance
    where 1 = 0
    {% endif %}

),

balances as (

    select
        s.account_id,
        s.balance_date,
        s.currency,
        coalesce(d.net_amount, 0)       as net_amount,
        coalesce(d.credit_amount, 0)    as credit_amount,
        coalesce(d.debit_amount, 0)     as debit_amount,
        coalesce(d.posted_txn_count, 0) as posted_txn_count,
        cast(
            coalesce(p.closing_balance, s.opening_balance)
            + sum(coalesce(d.net_amount, 0)) over (
                partition by s.account_id order by s.balance_date
                rows between unbounded preceding and current row
            )
            as decimal(18, 2)
        ) as closing_balance
    from spine s
    left join daily_activity d
        on d.account_id = s.account_id
       and d.txn_date = s.balance_date
    left join prior_balance p
        on p.account_id = s.account_id

)

select
    b.account_id,
    b.balance_date,
    b.currency,
    b.net_amount,
    b.credit_amount,
    b.debit_amount,
    b.posted_txn_count,
    b.closing_balance,
    cast(b.closing_balance * fx.rate_to_eur as decimal(18, 2)) as closing_balance_eur,
    {%- set loaded_through = "(select max(_loaded_at) from " ~ ref('fct_transactions') ~ ")" %}
    {#- Iceberg stores timestamps to the microsecond -#}
    {{ "cast(" ~ loaded_through ~ " as timestamp_ltz(6))" if as_iceberg else loaded_through }} as source_loaded_through
from balances b
left join {{ ref('int_fx_rates__daily') }} fx
    on fx.currency = b.currency
   and fx.rate_date = b.balance_date
