with
    int_date as (
        select
            {{ dbt_utils.generate_surrogate_key(['date_day', 'day_of_week_name']) }} as sk_date
            , *
        from {{ ref('stg_erp__date') }}
    )

select *
from int_date