with
    -- import models
    address as (
        select *
        from {{ ref('stg_erp__address') }}
    )
    ,  country_region as (
        select *
        from {{ ref('stg_erp__country_region') }}
    )
    ,  state_province as (
        select *
        from {{ ref('stg_erp__state_province') }}
    )
    ,  sales_territory as (
        select *
        from {{ ref('stg_erp__sales_territory') }}
    )
    -- transformation
    , joined as (
        select
            address.address_id
            , address.state_province_id
            , state_province.territory_id
            , state_province.state_province_code
            , state_province.country_region_code
            , address.city
            , address.address_line
            , state_province.province_name
            , country_region.country_region_name
            , sales_territory.territory_name
            , sales_territory.continent
        from address
        left join state_province 
            on state_province.state_province_id = address.state_province_id
        left join country_region
             on state_province.country_region_code = country_region.country_region_code
        left join sales_territory 
            on state_province.territory_id = sales_territory.territory_id
    )
    
select *
from joined