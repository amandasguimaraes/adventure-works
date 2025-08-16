with
    int_credit_card as (
        select 
            {{ dbt_utils.generate_surrogate_key(['credit_card_id', 'card_type']) }} as sk_credit_card
            , *
        from {{ ref('stg_erp__credit_card') }}
    )

select *
from int_credit_card