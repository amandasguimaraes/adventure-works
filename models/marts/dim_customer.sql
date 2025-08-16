with
    int_customer as (
        select
            {{ dbt_utils.generate_surrogate_key(['customer_id','business_entity_id']) }} as sk_customer
            , *
        from {{ ref('int_customer__enriched') }}
    )

select *
from int_customer