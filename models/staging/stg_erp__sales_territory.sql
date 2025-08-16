with
    source_sales_territory as (
        select *            
        from {{ source('erp','salesterritory') }}
    )
    , renamed as (
        select
            cast(territoryid as int) as territory_id
            , cast(countryregioncode as string) as country_region_code
            , cast(name as string) as territory_name
            , cast("group" as string) as continent
        from source_sales_territory
    )

select *
from renamed