-- Optional, Enterprise edition or higher. Masks PII for everyone except platform roles.
-- dbt applies the policies after each build when run with --vars '{enable_masking: true}'.

use role BANK_ADMIN;

create masking policy if not exists ANALYTICS.GOVERNANCE.MASK_EMAIL as (val varchar) returns varchar ->
    case
        when is_role_in_session('BANK_TRANSFORMER') then val
        else regexp_replace(val, '^(.).*(@.*)$', '\\1***\\2')
    end;

create masking policy if not exists ANALYTICS.GOVERNANCE.MASK_IBAN as (val varchar) returns varchar ->
    case
        when is_role_in_session('BANK_TRANSFORMER') then val
        else left(val, 4) || repeat('*', greatest(length(val) - 8, 0)) || right(val, 4)
    end;

grant apply on masking policy ANALYTICS.GOVERNANCE.MASK_EMAIL to role BANK_TRANSFORMER;
grant apply on masking policy ANALYTICS.GOVERNANCE.MASK_IBAN  to role BANK_TRANSFORMER;
