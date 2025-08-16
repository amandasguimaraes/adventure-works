with
    -- import models
    product as (
        select *
        from {{ ref('stg_erp__product') }}
    )
    ,  productsubcategory as (
        select *
        from {{ ref('stg_erp__product_subcategory') }}
    )
    ,  productcategory as (
        select *
        from {{ ref('stg_erp__product_category') }}
    )
    -- transformation
    , joined as (
        select
            product.product_id
            , product.productsubcategory_id
            , productsubcategory.productcategory_id
            , product.product_name
            , productcategory.productcategory_name
            , productsubcategory.productsubcategory_name
            , product.product_number
            , product.standard_cost
            , product.list_price
        from product
        left join productsubcategory 
            on product.productsubcategory_id = productsubcategory.productsubcategory_id
        left join productcategory 
            on productsubcategory.productcategory_id = productcategory.productcategory_id
    )
    
select *
from joined