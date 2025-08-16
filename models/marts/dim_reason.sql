with
    int_reason as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_id','reason_agg']) }} as sk_reason
            , *
        from {{ ref('int_reason__enriched') }}
    )

select *
from int_reason