{{ config(materialized='table') }}

{#-
    Transaction level output of three AML rules. Rolling windows are evaluated with
    equality joins on (account_id, bucket_date) instead of a pure range join, which keeps
    the pair count proportional to daily activity rather than to account history.
-#}

with card_attempts as (

    select txn_id, account_id, txn_ts, txn_date, amount_eur
    from {{ ref('fct_transactions') }}
    where txn_type = 'CARD_PAYMENT'

),

deposits as (

    select txn_id, account_id, txn_ts, txn_date, amount_eur
    from {{ ref('fct_transactions') }}
    where txn_type = 'CASH_DEPOSIT'
      and is_posted
      and amount_eur >= {{ var('aml_structuring_floor_eur') }}
      and amount_eur < {{ var('aml_structuring_ceiling_eur') }}

),

deposit_buckets as (

    select d.*, d.txn_date as bucket_date from deposits d
    union all
    select d.*, cast({{ dbt.dateadd('day', 1, 'd.txn_date') }} as date) from deposits d

),

card_buckets as (

    select c.*, c.txn_date as bucket_date from card_attempts c
    union all
    select c.*, cast({{ dbt.dateadd('day', 1, 'c.txn_date') }} as date) from card_attempts c

),

structuring_pairs as (

    select
        a.account_id,
        a.txn_id  as anchor_txn_id,
        a.txn_ts  as anchor_ts,
        w.txn_id,
        w.txn_ts,
        w.amount_eur
    from deposits a
    join deposit_buckets w
        on w.account_id = a.account_id
       and w.bucket_date = a.txn_date
       and w.txn_ts > {{ dbt.dateadd('hour', -var('aml_structuring_window_hours'), 'a.txn_ts') }}
       and w.txn_ts <= a.txn_ts

),

velocity_pairs as (

    select
        a.account_id,
        a.txn_id  as anchor_txn_id,
        a.txn_ts  as anchor_ts,
        w.txn_id,
        w.txn_ts,
        w.amount_eur
    from card_attempts a
    join card_buckets w
        on w.account_id = a.account_id
       and w.bucket_date = a.txn_date
       and w.txn_ts > {{ dbt.dateadd('minute', -var('aml_velocity_window_minutes'), 'a.txn_ts') }}
       and w.txn_ts <= a.txn_ts

),

window_pairs as (

    select 'STRUCTURING' as rule_code, p.*, {{ var('aml_structuring_window_hours') * 60 }} as window_minutes
    from structuring_pairs p
    where anchor_txn_id in (
        select anchor_txn_id from structuring_pairs
        group by anchor_txn_id
        having count(*) >= {{ var('aml_structuring_min_deposits') }}
    )

    union all

    select 'VELOCITY' as rule_code, p.*, {{ var('aml_velocity_window_minutes') }} as window_minutes
    from velocity_pairs p
    where anchor_txn_id in (
        select anchor_txn_id from velocity_pairs
        group by anchor_txn_id
        having count(*) >= {{ var('aml_velocity_min_payments') }}
    )

),

anchors as (

    select distinct rule_code, account_id, anchor_txn_id, anchor_ts, window_minutes
    from window_pairs

),

anchor_islands as (

    -- anchors closer together than the rule window belong to one alert
    select
        *,
        sum(is_new_alert) over (
            partition by rule_code, account_id order by anchor_ts, anchor_txn_id
            rows between unbounded preceding and current row
        ) as island
    from (
        select
            *,
            case
                when lag(anchor_ts) over (partition by rule_code, account_id order by anchor_ts, anchor_txn_id)
                     > {{ dbt.dateadd('minute', '-window_minutes', 'anchor_ts') }}
                then 0 else 1
            end as is_new_alert
        from anchors
    ) flagged

),

window_alerts as (

    select distinct
        i.rule_code,
        i.account_id,
        first_value(i.anchor_txn_id) over (
            partition by i.rule_code, i.account_id, i.island order by i.anchor_ts, i.anchor_txn_id
            rows between unbounded preceding and unbounded following
        ) as alert_anchor_txn_id,
        p.txn_id,
        p.txn_ts,
        p.amount_eur
    from anchor_islands i
    join window_pairs p
        on p.rule_code = i.rule_code
       and p.anchor_txn_id = i.anchor_txn_id

),

card_scored as (

    select
        txn_id,
        account_id,
        txn_ts,
        amount_eur,
        ln(amount_eur) as log_amount,
        avg(ln(amount_eur)) over (
            partition by account_id order by txn_ts, txn_id rows between 50 preceding and 1 preceding
        ) as history_mean_log,
        stddev_samp(ln(amount_eur)) over (
            partition by account_id order by txn_ts, txn_id rows between 50 preceding and 1 preceding
        ) as history_sd_log,
        count(*) over (
            partition by account_id order by txn_ts, txn_id rows between 50 preceding and 1 preceding
        ) as history_count
    from {{ ref('fct_transactions') }}
    where txn_type = 'CARD_PAYMENT'
      and is_posted
      and amount_eur > 0

),

outlier_alerts as (

    select
        'OUTLIER' as rule_code,
        account_id,
        txn_id as alert_anchor_txn_id,
        txn_id,
        txn_ts,
        amount_eur
    from card_scored
    where history_count >= {{ var('aml_outlier_min_history') }}
      and amount_eur >= {{ var('aml_outlier_min_amount_eur') }}
      and (log_amount - history_mean_log) / nullif(history_sd_log, 0) >= {{ var('aml_outlier_min_zscore') }}

),

all_hits as (

    select * from window_alerts
    union all
    select * from outlier_alerts

)

select
    {{ surrogate_key(['rule_code', 'account_id', 'alert_anchor_txn_id']) }} as alert_id,
    rule_code,
    account_id,
    alert_anchor_txn_id,
    txn_id,
    txn_ts,
    amount_eur
from all_hits
