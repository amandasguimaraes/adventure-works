with
    int_reason as (
        select
             {{ dbt_utils.generate_surrogate_key(['sales_order_id']) }} as sk_reason
            , *
        from {{ ref('int_reason__aggregated') }}
    )

select *
from int_reason