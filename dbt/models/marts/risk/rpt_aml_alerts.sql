with alerts as (

    select
        alert_id,
        rule_code,
        account_id,
        min(txn_ts)     as alert_start_ts,
        max(txn_ts)     as alert_end_ts,
        count(*)        as txn_count,
        sum(amount_eur) as total_amount_eur
    from {{ ref('int_aml__alert_transactions') }}
    group by alert_id, rule_code, account_id

)

select
    a.alert_id,
    a.rule_code,
    a.account_id,
    acc.customer_id,
    c.segment                           as customer_segment_at_alert,
    c.risk_rating                       as customer_risk_rating_at_alert,
    a.alert_start_ts,
    a.alert_end_ts,
    cast(a.alert_end_ts as date)        as alert_date,
    a.txn_count,
    a.total_amount_eur,
    case
        when a.rule_code = 'STRUCTURING' or c.risk_rating = 'HIGH' then 'HIGH'
        when a.total_amount_eur >= 5000 then 'MEDIUM'
        else 'LOW'
    end as severity
from alerts a
inner join {{ ref('dim_account') }} acc
    on acc.account_id = a.account_id
left join {{ ref('dim_customer') }} c
    on c.customer_id = acc.customer_id
   and a.alert_end_ts >= c.valid_from
   and (c.valid_to is null or a.alert_end_ts < c.valid_to)
