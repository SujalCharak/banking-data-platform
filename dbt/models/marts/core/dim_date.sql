with days as (

    {{ dbt.date_spine(
        'day',
        "cast('" ~ var('calendar_start') ~ "' as date)",
        "cast('" ~ var('calendar_end') ~ "' as date)"
    ) }}

)

select
    cast(date_day as date)                                         as date_day,
    cast(extract(year from date_day) as integer)                   as year_number,
    cast(extract(quarter from date_day) as integer)                as quarter_number,
    cast(extract(month from date_day) as integer)                  as month_number,
    cast(extract(day from date_day) as integer)                    as day_of_month,
    cast({{ iso_day_of_week('date_day') }} as integer)             as iso_day_of_week,
    {{ iso_day_of_week('date_day') }} in (6, 7)                    as is_weekend,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date)        as month_start_date,
    cast({{ dbt.last_day('date_day', 'month') }} as date)          as month_end_date
from days
