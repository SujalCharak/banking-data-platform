-- Run as ACCOUNTADMIN. Creates roles, warehouses, a spend cap and the service user.

use role accountadmin;

create role if not exists BANK_ADMIN       comment = 'Owns the platform databases, procedures and tasks';
create role if not exists BANK_LOADER      comment = 'Uploads landing files and runs COPY into RAW';
create role if not exists BANK_TRANSFORMER comment = 'Runs dbt: reads RAW, builds ANALYTICS';
create role if not exists BANK_ANALYST     comment = 'Read only access to ANALYTICS marts';

grant role BANK_ANALYST     to role BANK_TRANSFORMER;
grant role BANK_LOADER      to role BANK_ADMIN;
grant role BANK_TRANSFORMER to role BANK_ADMIN;
grant role BANK_ADMIN       to role SYSADMIN;

set my_user = current_user();
grant role BANK_ADMIN to user identifier($my_user);

create warehouse if not exists BANK_LOAD_WH
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    comment = 'COPY and stored procedures';
create warehouse if not exists BANK_TRANSFORM_WH
    warehouse_size = small auto_suspend = 60 auto_resume = true initially_suspended = true
    comment = 'dbt builds';
create warehouse if not exists BANK_ANALYST_WH
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    comment = 'Ad hoc analysis';
create warehouse if not exists BANK_BENCH_WH
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    comment = 'Performance benchmarks; resized by the benchmark script';

create resource monitor if not exists BANK_MONTHLY_CAP
    with credit_quota = 40
    frequency = monthly
    start_timestamp = immediately
    triggers on 75 percent do notify
             on 90 percent do suspend
             on 100 percent do suspend_immediate;

alter warehouse BANK_LOAD_WH      set resource_monitor = BANK_MONTHLY_CAP;
alter warehouse BANK_TRANSFORM_WH set resource_monitor = BANK_MONTHLY_CAP;
alter warehouse BANK_ANALYST_WH   set resource_monitor = BANK_MONTHLY_CAP;
alter warehouse BANK_BENCH_WH     set resource_monitor = BANK_MONTHLY_CAP;

grant usage, operate on warehouse BANK_LOAD_WH              to role BANK_LOADER;
grant usage, operate on warehouse BANK_TRANSFORM_WH         to role BANK_TRANSFORMER;
grant usage          on warehouse BANK_ANALYST_WH           to role BANK_ANALYST;
grant usage, operate, modify on warehouse BANK_BENCH_WH     to role BANK_ADMIN;

grant create database        on account to role BANK_ADMIN;
grant execute task           on account to role BANK_ADMIN;
grant execute managed task   on account to role BANK_ADMIN;
grant imported privileges    on database SNOWFLAKE to role BANK_ADMIN;

-- Service user for dbt and the Python tooling. Key pair auth only; set the public key after creating it (see docs/snowflake_runbook.md).
create user if not exists BANK_SVC
    type = service
    default_role = BANK_ADMIN
    default_warehouse = BANK_TRANSFORM_WH
    comment = 'Pipeline service account';
grant role BANK_ADMIN to user BANK_SVC;
