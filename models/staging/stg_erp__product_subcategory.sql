with
    source_product_subcategory as (
        select *
        from {{ source('erp','production_productsubcategory') }}
    )
    , renamed as (
        select 
            cast(productsubcategoryid as int) as productsubcategory_id
            , cast(productcategoryid as int) as productcategory_id 
            , cast(name as string) as productsubcategory_name
        from source_product_subcategory
    )

select *
from renamed