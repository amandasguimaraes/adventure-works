with
    source_state_province as (
        select *            
        from {{ source('erp','stateprovince') }}
    )
    , renamed as (
        select
            cast(stateprovincecode as string) as state_province_code
            , cast(name as string) as province_name
            , cast(countryregioncode as string) as country_region_code
            , cast(stateprovinceid as int) as state_province_id
            , cast(territoryid as int) as territory_id
        from source_state_province
    )

select *
from renamed