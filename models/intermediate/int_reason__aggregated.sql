with
    -- import models
    sales_order_header_sales_reason as (
        select *
        from {{ ref('stg_erp__sales_order_header_sales_reason') }}
    )
    ,  sales_reason as (
        select *
        from {{ ref('stg_erp__sales_reason') }}
    )
    -- transformation
    , joined as (
        select
            sales_order_header_sales_reason.sales_order_id
            , listagg(sales_reason.reason_name, ', ') within group (order by sales_reason.reason_name) as reasons
        from sales_order_header_sales_reason
        left join sales_reason 
            on sales_order_header_sales_reason.sales_reason_id = sales_reason.sales_reason_id
        group by sales_order_header_sales_reason.sales_order_id
    )
    
select *
from joined