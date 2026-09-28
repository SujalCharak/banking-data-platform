with versions as (

    select *
    from {{ ref('stg_bank__customers') }}
    qualify row_number() over (
        partition by customer_id, updated_at
        order by _source_file desc, _source_row desc
    ) = 1

),

hashed as (

    select
        *,
        {{ surrogate_key(['first_name', 'last_name', 'email', 'country_code', 'segment', 'risk_rating', 'kyc_status']) }}
            as attribute_hash
    from versions

),

real_changes as (

    select *
    from hashed
    qualify coalesce(lag(attribute_hash) over (partition by customer_id order by updated_at), '') <> attribute_hash

)

select
    {{ surrogate_key(['customer_id', 'updated_at']) }} as customer_version_key,
    customer_id,
    first_name,
    last_name,
    email,
    birth_date,
    country_code,
    segment,
    risk_rating,
    kyc_status,
    created_at,
    updated_at                                                          as valid_from,
    lead(updated_at) over (partition by customer_id order by updated_at) as valid_to,
    attribute_hash
from real_changes
