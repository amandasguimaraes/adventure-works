with
    int_reason as (
        select
             {{ dbt_utils.generate_surrogate_key(['sales_order_id','sales_reason_agg']) }} as sk_reason
            , sales_order_id
            , sales_reason_agg
        from {{ ref('int_reason__aggregated') }}
    )

select *
from int_reason