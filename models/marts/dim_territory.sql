with
    int_territory as (
        select
            {{ dbt_utils.generate_surrogate_key(['address_id']) }} as sk_territory
            , *
        from {{ ref('int_territory__enriched') }}
    )
select *
from int_territory