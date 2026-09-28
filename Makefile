.PHONY: install local small test lint dbt-docs sf-load sf-build sf-validate benchmark cost

DBT = cd dbt && dbt
DBT_ARGS = --profiles-dir .

install:
	pip install -e ".[dev,snowflake]"

local:
	bankdp run-local

small:
	bankdp run-local --customers 800 --days 120

test:
	pytest -q

lint:
	ruff check src tests
	ruff format --check src tests

dbt-docs:
	$(DBT) docs generate $(DBT_ARGS) && dbt docs serve $(DBT_ARGS)

sf-load:
	bankdp load-snowflake

sf-build:
	$(DBT) seed --target prod $(DBT_ARGS) && dbt build --target prod $(DBT_ARGS)

sf-validate:
	bankdp validate --target snowflake --out docs/validation_report_snowflake.md

benchmark:
	bankdp benchmark

cost:
	bankdp cost-report
