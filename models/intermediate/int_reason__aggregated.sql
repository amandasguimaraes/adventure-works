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
    , reason_order as (
        select
            sales_order_header_sales_reason.sales_order_id
            , sales_reason.sales_reason_id
            , sales_reason.reason_name
            , sales_reason.reason_type
        from sales_order_header_sales_reason
        left join sales_reason 
            on sales_order_header_sales_reason.sales_reason_id = sales_reason.sales_reason_id
    )
    , joined as (
        select
            sales_order_id
            , coalesce(listagg(reason_name, ', ') within group (order by reason_name), 'No Reason') as sales_reason_agg
        from reason_order
        group by sales_order_id
    )
    
select *
from joined