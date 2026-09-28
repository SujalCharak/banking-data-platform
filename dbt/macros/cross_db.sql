{#
    Cross database helpers. Formats are written once in strftime style and
    translated for Snowflake, so models stay identical across both targets.
#}

{% macro to_snowflake_format(fmt) -%}
    {{- fmt | replace('%Y', 'YYYY') | replace('%m', 'MM') | replace('%d', 'DD')
            | replace('%H', 'HH24') | replace('%M', 'MI') | replace('%S', 'SS') -}}
{%- endmacro %}


{% macro try_parse_timestamp(expr, formats) -%}
    {{ return(adapter.dispatch('try_parse_timestamp', 'bank_platform')(expr, formats)) }}
{%- endmacro %}

{% macro default__try_parse_timestamp(expr, formats) -%}
    coalesce(
    {%- for fmt in formats %}
        try_to_timestamp_ntz({{ expr }}, '{{ bank_platform.to_snowflake_format(fmt) }}'){{ "," if not loop.last }}
    {%- endfor %}
    )
{%- endmacro %}

{% macro duckdb__try_parse_timestamp(expr, formats) -%}
    coalesce(
    {%- for fmt in formats %}
        try_strptime({{ expr }}, '{{ fmt }}'){{ "," if not loop.last }}
    {%- endfor %}
    )
{%- endmacro %}


{% macro try_parse_date(expr, fmt='%Y-%m-%d') -%}
    {{ return(adapter.dispatch('try_parse_date', 'bank_platform')(expr, fmt)) }}
{%- endmacro %}

{% macro default__try_parse_date(expr, fmt) -%}
    try_to_date({{ expr }}, '{{ bank_platform.to_snowflake_format(fmt) }}')
{%- endmacro %}

{% macro duckdb__try_parse_date(expr, fmt) -%}
    cast(try_strptime({{ expr }}, '{{ fmt }}') as date)
{%- endmacro %}


{% macro regex_full_match(expr, pattern) -%}
    {{ return(adapter.dispatch('regex_full_match', 'bank_platform')(expr, pattern)) }}
{%- endmacro %}

{% macro default__regex_full_match(expr, pattern) -%}
    regexp_like({{ expr }}, '{{ pattern }}')
{%- endmacro %}

{% macro duckdb__regex_full_match(expr, pattern) -%}
    regexp_full_match({{ expr }}, '{{ pattern }}')
{%- endmacro %}


{% macro iso_day_of_week(expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week', 'bank_platform')(expr)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(expr) -%}
    dayofweekiso({{ expr }})
{%- endmacro %}

{% macro duckdb__iso_day_of_week(expr) -%}
    isodow({{ expr }})
{%- endmacro %}


{% macro surrogate_key(columns) -%}
    {%- set parts = [] -%}
    {%- for col in columns -%}
        {%- do parts.append("coalesce(cast(" ~ col ~ " as " ~ dbt.type_string() ~ "), '_null_')") -%}
        {%- if not loop.last %}{% do parts.append("'|'") %}{% endif -%}
    {%- endfor -%}
    {{ dbt.hash(dbt.concat(parts)) }}
{%- endmacro %}


{% macro integer_series(upper_bound_exclusive=10000) -%}
    {#- Portable number generator: cross join of digit tables, 0 to 9999. -#}
    select d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000 as n
    from (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
          union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) d1
    cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
          union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) d2
    cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
          union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) d3
    cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
          union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) d4
    where d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000 < {{ upper_bound_exclusive }}
{%- endmacro %}


{% macro merge_strategy() -%}
    {{- 'merge' if target.type == 'snowflake' else 'delete+insert' -}}
{%- endmacro %}


{% macro loaded_since_last_run(column='_loaded_at') -%}
    {#- Reprocesses a short lookback so loads that landed during the previous run are not missed; merges make this idempotent. -#}
    {{ column }} > (
        select coalesce(
            {{ dbt.dateadd('hour', -var('incremental_lookback_hours'), 'max(' ~ column ~ ')') }},
            cast('1900-01-01' as timestamp)
        )
        from {{ this }}
    )
{%- endmacro %}
