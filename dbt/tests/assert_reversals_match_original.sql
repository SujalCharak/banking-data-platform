{{ config(severity='warn') }}

-- A reversal must point at a posted card payment on the same account for the same amount.

select r.txn_id, r.original_txn_id
from {{ ref('fct_transactions') }} r
left join {{ ref('fct_transactions') }} o
    on o.txn_id = r.original_txn_id
where r.txn_type = 'REVERSAL'
  and (
      o.txn_id is null
      or o.account_id <> r.account_id
      or o.amount <> r.amount
      or o.txn_type <> 'CARD_PAYMENT'
  )
