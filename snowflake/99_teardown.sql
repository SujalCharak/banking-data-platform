-- Removes everything the setup scripts created. Run as ACCOUNTADMIN.

use role accountadmin;

drop database if exists ANALYTICS;
drop database if exists RAW;
drop warehouse if exists BANK_LOAD_WH;
drop warehouse if exists BANK_TRANSFORM_WH;
drop warehouse if exists BANK_ANALYST_WH;
drop warehouse if exists BANK_BENCH_WH;
drop resource monitor if exists BANK_MONTHLY_CAP;
drop user if exists BANK_SVC;
drop role if exists BANK_ANALYST;
drop role if exists BANK_TRANSFORMER;
drop role if exists BANK_LOADER;
drop role if exists BANK_ADMIN;
