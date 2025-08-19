with
    source_product as (
        select *            
        from {{ source('erp','production_product') }}
    )
    , renamed as (
        select
            cast(productid as int) as product_id
            , cast(name as string) as product_name
            , cast(productnumber as string) as product_number
            , cast(productsubcategoryid as int) as productsubcategory_id
            , cast(standardcost as numeric(18,2)) as standard_cost
            , cast(listprice as numeric(18,2)) as list_price
        from source_product
    )

select *
from renamed