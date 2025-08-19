with
    source_product_category as (
        select *            
        from {{ source('erp','production_productcategory') }}
    )
    , renamed as (
        select
            cast(productcategoryid as int) as productcategory_id
            , cast(name as string) as productcategory_name
        from source_product_category
    )

select *
from renamed