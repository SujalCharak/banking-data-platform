-- The last closing balance must equal opening balance plus all posted movements.

with last_balance as (
    select account_id, closing_balance
    from {{ ref('fct_account_daily_balance') }}
    qualify row_number() over (partition by account_id order by balance_date desc) = 1
),

expected as (
    select
        a.account_id,
        a.opening_balance + coalesce(sum(t.signed_amount), 0) as expected_balance
    from {{ ref('dim_account') }} a
    left join {{ ref('fct_transactions') }} t
        on t.account_id = a.account_id
       and t.is_posted
    group by a.account_id, a.opening_balance
)

select e.account_id, e.expected_balance, l.closing_balance
from expected e
inner join last_balance l
    on l.account_id = e.account_id
where abs(e.expected_balance - l.closing_balance) > 0.005
