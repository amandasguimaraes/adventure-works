with
    source_country_region as (
        select *            
        from {{ source('erp','person_countryregion') }}
    )
    , renamed as (
        select
            cast(countryregioncode as string) as country_region_code
            , cast(name as string) as country_region_name
        from source_country_region
    )

select *
from renamed