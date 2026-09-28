{% macro search_optimization(columns) -%}
    {#- Adds equality search optimization if the table does not have it yet. Enterprise edition only. -#}
    {%- set enabled = false -%}
    {%- if target.type == 'snowflake' and var('enable_search_optimization') and execute -%}
        {%- set shown = run_query("show tables like '" ~ this.identifier ~ "' in schema " ~ this.database ~ "." ~ this.schema) -%}
        {%- set enabled = shown.rows | length > 0 and shown.columns['search_optimization'].values()[0] == 'ON' -%}
    {%- endif -%}
    {%- if target.type == 'snowflake' and var('enable_search_optimization') and not enabled -%}
        alter table {{ this }} add search optimization on equality({{ columns | join(', ') }})
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}


{% macro apply_masking_policy(column, policy) -%}
    {#- Policies live in ANALYTICS.GOVERNANCE (snowflake/05_governance_optional.sql). Enterprise edition only. -#}
    {%- if target.type == 'snowflake' and var('enable_masking') -%}
        alter table {{ this }} modify column {{ column }} set masking policy ANALYTICS.GOVERNANCE.{{ policy }}
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
