{#
    Override of dbt-snowflake's CREATE ICEBERG TABLE for Snowflake as the catalog.
    dbt-snowflake 1.11 always sends BASE_LOCATION, which Snowflake rejects for tables on
    Snowflake managed storage (dbt-labs/dbt-adapters#1911). Same DDL otherwise; drop this file
    once the adapter omits it.
#}
{% macro snowflake__create_table_built_in_sql(relation, compiled_code) -%}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
{%- set snowflake_managed = (catalog_relation.external_volume or '') | upper == 'SNOWFLAKE_MANAGED' -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace iceberg table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {{ optional('external_volume', catalog_relation.external_volume, "'") }}
    catalog = 'SNOWFLAKE'
    {%- if not snowflake_managed %}
    base_location = '{{ catalog_relation.base_location }}'
    {%- endif %}
    {{ optional('storage_serialization_policy', catalog_relation.storage_serialization_policy, "'") }}
    {{ optional('max_data_extension_time_in_days', catalog_relation.max_data_extension_time_in_days) }}
    {{ optional('data_retention_time_in_days', catalog_relation.data_retention_time_in_days) }}
    {{ optional('change_tracking', catalog_relation.change_tracking) }}
as (
    {%- if catalog_relation.cluster_by is not none -%}
    select * from (
        {{ compiled_code }}
    )
    order by (
        {{ catalog_relation.cluster_by }}
    )
    {%- else -%}
    {{ compiled_code }}
    {%- endif %}
    )
;

{% if catalog_relation.cluster_by is not none -%}
alter iceberg table {{ relation }} cluster by ({{ catalog_relation.cluster_by }});
{%- endif -%}

{%- endmacro %}
