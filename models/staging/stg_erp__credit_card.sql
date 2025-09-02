with
    source_credit_card as (
        select *            
        from {{ source('erp','sales_creditcard') }}
    )
    , renamed as (
        select
            cast(creditcardid as int) as credit_card_id
            , cast(cardtype as string) as card_type
            , cast(cardnumber as string) as card_number
        from source_credit_card
    )

select *
from renamed