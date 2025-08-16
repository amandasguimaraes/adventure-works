with
    -- import models
     sales_order_detail as (
        select *
        from {{ref('stg_erp__sales_order_detail')}}
    ) 
    , sales_order_header as (
        select *
        from {{ref('stg_erp__sales_order_header')}}
    )  
    -- transformation
    , joined as (
        select
            sales_order_header.sales_order_id
            , sales_order_header.sales_order_detail_id
            , sales_order_header.customer_id
            , sales_order_header.credit_card_id
            , sales_order_detail.product_id
            , sales_order_header.ship_to_address_id
            , sales_order_header.order_date
            , sales_order_header.due_date
            , sales_order_header.ship_date
            , sales_order_detail.order_quantity
            , sales_order_detail.unit_price
            , sales_order_detail.unit_price_discount
            , sales_order_header.freight
            , sales_order_header.tax
            , sales_order_header.sub_total
            , sales_order_header.total_due
            , sales_order_header.sales_type
            , sales_order_header.order_status
        from sales_order_header
        left join sales_order_detail 
            on sales_order_header.sales_order_id = sales_order_detail.sales_order_id
    )
    , metrics as (
        select
            sales_order_detail.unit_price * sales_order_detail.order_quantity as gross_total
            sales_order_detail.unit_price * (1 - sales_order_detail.unit_price-discount) * sales_order_detail.order_quantity as net_total
            , case
                when order_detail.unit_price_discount > 0 then true
                else false
            end as had_discount
        from joined
    )

select *
from metrics