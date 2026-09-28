-- Run as ACCOUNTADMIN (needs MANAGE GRANTS). Lets analysts read everything dbt builds, now and later.

use role accountadmin;

grant usage  on future schemas in database ANALYTICS to role BANK_ANALYST;
grant select on future tables  in database ANALYTICS to role BANK_ANALYST;
grant select on future views   in database ANALYTICS to role BANK_ANALYST;

grant select on future tables in schema RAW.BANK to role BANK_TRANSFORMER;
grant select, insert on future tables in schema RAW.BANK to role BANK_LOADER;
