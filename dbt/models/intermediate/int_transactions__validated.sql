with transactions as (

    select * from {{ ref('stg_bank__transactions') }}

),

accounts as (

    select account_id, currency from {{ ref('int_accounts__current') }}

)

select
    t.*,
    a.currency as account_currency,
    case
        when t.txn_id is null                           then 'MISSING_TXN_ID'
        when t.txn_ts_raw is null                       then 'MISSING_TIMESTAMP'
        when t.txn_ts is null                           then 'INVALID_TIMESTAMP'
        when t.amount is null                           then 'INVALID_AMOUNT'
        when t.amount < 0                               then 'NEGATIVE_AMOUNT'
        when t.amount = 0                               then 'ZERO_AMOUNT'
        when a.account_id is null                       then 'UNKNOWN_ACCOUNT'
        when t.direction is null
          or t.direction not in ('DR', 'CR')            then 'INVALID_DIRECTION'
        when t.currency <> a.currency                   then 'CURRENCY_MISMATCH'
    end as rejection_reason,
    case
        when t.txn_type = 'CARD_PAYMENT' and t.merchant_id is null then 'MISSING_MERCHANT'
    end as warning_reason
from transactions t
left join accounts a
    on t.account_id = a.account_id
