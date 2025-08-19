with
    source_address as (
        select *            
        from {{ source('erp','person_address') }}
    )
    , renamed as (
        select
            cast(addressid as int) as address_id
            , cast(city as string) as city
            , cast(stateprovinceid as int) as state_province_id
            , cast(addressline1 as string) as address_line
        from source_address
    )

select *
from renamed