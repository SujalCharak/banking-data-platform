{{
    config(
        materialized='incremental',
        unique_key='txn_id',
        incremental_strategy=merge_strategy(),
        on_schema_change='append_new_columns',
        cluster_by=['txn_date'],
        transient=false,
        post_hook="{{ bank_platform.search_optimization(['txn_id', 'account_id']) }}"
    )
}}

with batch as (

    select *
    from {{ ref('int_transactions__validated') }}
    where rejection_reason is null
    {% if is_incremental() %}
      and {{ loaded_since_last_run() }}
    {% endif %}

),

latest_version as (

    -- a transaction delivered more than once keeps the version from the latest extract; file names carry the
    -- extract date, while _loaded_at is per file scan time on Snowflake and can be out of order within one COPY
    select *
    from batch
    qualify row_number() over (
        partition by txn_id
        order by _source_file desc, _loaded_at desc, _source_row desc
    ) = 1

),

accounts as (

    select account_id, customer_id from {{ ref('int_accounts__current') }}

),

fx as (

    select rate_date, currency, rate_to_eur, published_date
    from {{ ref('int_fx_rates__daily') }}

),

types as (

    select txn_type, txn_category from {{ ref('transaction_types') }}

)

select
    t.txn_id,
    t.account_id,
    a.customer_id,
    t.txn_ts,
    cast(t.txn_ts as date)                                                          as txn_date,
    t.txn_type,
    ty.txn_category,
    t.direction,
    t.status,
    t.channel,
    t.merchant_id,
    t.counterparty_iban,
    t.original_txn_id,
    t.currency,
    t.amount,
    case when t.direction = 'DR' then -t.amount else t.amount end                   as signed_amount,
    fx.rate_to_eur                                                                  as fx_rate_to_eur,
    fx.published_date                                                               as fx_rate_published_date,
    cast(t.amount * fx.rate_to_eur as decimal(18, 2))                               as amount_eur,
    cast(
        case when t.direction = 'DR' then -t.amount else t.amount end * fx.rate_to_eur
        as decimal(18, 2)
    )                                                                               as signed_amount_eur,
    t.status = 'POSTED'                                                             as is_posted,
    t.warning_reason,
    t.device_id,
    t.description,
    t._source_file,
    t._loaded_at
from latest_version t
inner join accounts a
    on a.account_id = t.account_id
left join fx
    on fx.currency = t.currency
   and fx.rate_date = cast(t.txn_ts as date)
left join types ty
    on ty.txn_type = t.txn_type
