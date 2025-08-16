with
    int_product as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_id','product_number']) }} as sk_product
            , *
        from {{ ref('int_product__enriched') }}
    )

select *
from int_product