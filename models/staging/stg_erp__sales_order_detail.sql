with
    source_sales_order_detail as (
        select *            
        from {{ source('erp','sales_salesorderdetail') }}
    )
    , renamed as (
        select
            cast(salesorderid as int) as sales_order_id	
            , cast(salesorderdetailid as int) as sales_order_detail_id
            , cast(orderqty as int) as order_quantity
            , cast(productid as int) as product_id
            , cast(unitprice as numeric) as unit_price
            , cast(unitpricediscount as numeric) as unit_price_discount
        from source_sales_order_detail
    )

select *
from renamed