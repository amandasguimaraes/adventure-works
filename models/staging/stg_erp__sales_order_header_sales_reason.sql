with
    source_sales_order_header_sales_reason as (
        select *            
        from {{ source('erp','salesorderheadersalesreason') }}
    )
    , renamed as (
        select
            cast(salesorderid as int) as sales_order_id
            , cast(salesreasonid as int) as sales_reason_id
        from source_sales_order_header_sales_reason
    )

select *
from renamed