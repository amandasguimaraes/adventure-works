with
    -- import models
    sales_order_detail as (
        select * 
        from {{ ref('stg_erp__sales_order_detail') }}
    )
    , sales_order_header as (
        select * 
        from {{ ref('stg_erp__sales_order_header') }}
    )
    -- transformation
    , joined as (
        select
            sales_order_header.sales_order_id
            , sales_order_detail.sales_order_detail_id
            , sales_order_detail.product_id
            , sales_order_header.order_date
            , sales_order_header.due_date
            , sales_order_header.ship_date
            , sales_order_header.customer_id
            , sales_order_header.territory_id
            , sales_order_header.sales_person_id
            , sales_order_header.ship_to_address_id
            , sales_order_header.bill_to_address_id
            , sales_order_header.credit_card_id
            , sales_order_header.order_status
            , sales_order_header.sales_type
            , sales_order_header.freight 
            , sales_order_header.tax 
            , sales_order_header.sub_total
            , sales_order_header.total_due
            , sales_order_detail.order_quantity
            , sales_order_detail.unit_price
            , sales_order_detail.unit_price_discount
        from sales_order_detail 
        left join sales_order_header 
            on sales_order_detail.sales_order_id = sales_order_header.sales_order_id
    )
    , metrics as (
        select
            sales_order_id
            , sales_order_detail_id
            , customer_id
            , credit_card_id
            , product_id
            , territory_id
            , sales_person_id
            , ship_to_address_id
            , bill_to_address_id
            , order_status
            , sales_type
            , total_due
            , order_quantity
            , unit_price
            , unit_price_discount
            , (unit_price * order_quantity) as gross_total
            , (unit_price * (1 - unit_price_discount) * order_quantity) as net_total
            , freight / count(*) over(partition by sales_order_id) as prorated_freight
            , tax / count(*) over(partition by sales_order_id) as prorated_tax
            , (unit_price * unit_price_discount * order_quantity) as discount_amount
            , case when unit_price_discount > 0 then true else false end as had_discount
            , (net_total + prorated_freight + prorated_tax) as line_total
            , order_date
            , ship_date
            , due_date
        from joined
    )

select *
from metrics
