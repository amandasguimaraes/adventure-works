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
    , joined_order_detail as (
        select
            sales_order_detail.sales_order_id
            , sales_order_detail.product_id
            , sales_order_detail.order_quantity
            , sales_order_detail.unit_price
            , sales_order_detail.unit_price_discount
            , (sales_order_detail.unit_price * sales_order_detail.order_quantity) as gross_total
            , (sales_order_detail.unit_price * (1 - sales_order_detail.unit_price_discount) * sales_order_detail.order_quantity) as net_total
            , (sales_order_detail.unit_price * sales_order_detail.unit_price_discount * sales_order_detail.order_quantity) as discount_amount
            , case
                when sales_order_detail.unit_price_discount > 0 then true
                else false
            end as had_discount
        from sales_order_detail
    )
    , joined_order_header as (
        select
            sales_order_header.sales_order_id
            , sales_order_header.customer_id
            , sales_order_header.territory_id
            , sales_order_header.credit_card_id
            , sales_order_header.order_status
            , sales_order_header.online_order
            , sales_order_header.freight
            , sales_order_header.tax
            , sales_order_header.sub_total
            , sales_order_header.total_due
            , sales_order_header.order_date
            , sales_order_header.ship_date
            , sales_order_header.due_date
        from sales_order_header
    )
    , metrics as (
        select
            {{ dbt_utils.generate_surrogate_key(['joined_order_detail.sales_order_id']) }} as sk_sales_order
            , joined_order_detail.sales_order_id
            , joined_order_detail.product_id
            , joined_order_detail.order_quantity
            , joined_order_detail.unit_price
            , joined_order_detail.unit_price_discount
            , joined_order_detail.gross_total
            , joined_order_detail.net_total
            , joined_order_detail.discount_amount
            , joined_order_detail.had_discount
            , joined_order_header.customer_id
            , joined_order_header.territory_id
            , joined_order_header.credit_card_id
            , joined_order_header.order_status
            , joined_order_header.online_order
            , joined_order_header.freight
            , joined_order_header.tax
            , joined_order_header.sub_total
            , joined_order_header.total_due
            , joined_order_header.order_date
            , joined_order_header.ship_date
            , joined_order_header.due_date
        from joined_order_detail
        left join joined_order_header 
            on joined_order_detail.sales_order_id = joined_order_header.sales_order_id
    )

select *
from metrics