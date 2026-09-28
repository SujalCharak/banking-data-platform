-- Every delivered row must end up in exactly one place: the fact table (once per txn_id) or quarantine.

with delivered as (
    select count(*) as n from {{ source('bank', 'transactions') }}
),

validated as (
    select
        count(*)                                                                as n,
        count(distinct case when rejection_reason is null then txn_id end)      as valid_distinct,
        sum(case when rejection_reason is not null then 1 else 0 end)           as rejected
    from {{ ref('int_transactions__validated') }}
),

facts as (
    select count(*) as n from {{ ref('fct_transactions') }}
),

quarantine as (
    select count(*) as n from {{ ref('dq_rejected_transactions') }}
)

select 'validation_changed_row_count' as check_name, d.n as expected, v.n as actual
from delivered d cross join validated v where d.n <> v.n
union all
select 'fact_rows_vs_valid_txn_ids', v.valid_distinct, f.n
from validated v cross join facts f where v.valid_distinct <> f.n
union all
select 'quarantine_rows_vs_rejected', v.rejected, q.n
from validated v cross join quarantine q where v.rejected <> q.n
