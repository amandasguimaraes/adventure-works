with
    source_state_province as (
        select *            
        from {{ source('erp','person_stateprovince') }}
    )
    , renamed as (
        select
            cast(stateprovinceid as int) as state_province_id
            , cast(stateprovincecode as string) as state_province_code
            , cast(countryregioncode as string) as country_region_code
            , cast(territoryid as int) as territory_id
            , cast(name as string) as province_name
        from source_state_province
    )

select *
from renamed