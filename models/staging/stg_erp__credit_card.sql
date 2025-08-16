with
    source_credit_card as (
        select *            
        from {{ source('erp','creditcard') }}
    )
    , renamed as (
        select
            cast(creditcardid as int) as credit_card_id
            , cast(cardtype as string) as card_type
        from source_credit_card
    )

select *
from renamed