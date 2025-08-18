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
    , calendar as (
        select *
        from {{ ref('dim_date') }}
    )
    , credit_card as (
        select *
        from {{ ref('dim_credit_card') }}
    )
    , customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    , territory as (
        select *
        from {{ ref('dim_territory') }}
    )
    , product as (
        select *
        from {{ ref('dim_product') }}
    )
    , reason as (
        select *
        from {{ ref('int_reason__aggregated') }}
    )
    -- transformation
    , joined as (
        select
            sales_order_header.sales_order_id
            , sales_order_detail.sales_order_detail_id
            , sales_order_header.customer_id
            , sales_order_header.credit_card_id
            , sales_order_detail.product_id
            , sales_order_header.territory_id
            , sales_order_header.ship_to_address_id
            , customer.sk_customer as fk_customer
            , coalesce(credit_card.sk_credit_card, -1) as fk_credit_card
            , product.sk_product as fk_product
            , territory.sk_territory as fk_territory
            , calendar.sk_date as fk_date
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
            , sales_order_header.online_order
            , sales_order_detail.unit_price * sales_order_detail.order_quantity as gross_total
            , sales_order_detail.unit_price * (1 - sales_order_detail.unit_price_discount) * sales_order_detail.order_quantity as net_total
            , sales_order_detail.unit_price * sales_order_detail.unit_price_discount * sales_order_detail.order_quantity as discount_amount
            , case
                when sales_order_detail.unit_price_discount > 0 then true
                else false
            end as had_discount
        from sales_order_header
        left join sales_order_detail 
            on sales_order_header.sales_order_id = sales_order_detail.sales_order_id
        left join customer
            on sales_order_header.customer_id = customer.customer_id
        left join credit_card
            on sales_order_header.credit_card_id = credit_card.credit_card_id
        left join product
            on sales_order_detail.product_id = product.product_id
        left join territory
            on sales_order_header.territory_id = territory.territory_id
        left join calendar
            on sales_order_header.order_date = calendar.date_day
        left join reason
            on sales_order_header.sales_order_id = reason.sales_order_id 
    )
    , metrics as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_id', 'sales_order_detail_id']) }} as sk_sales_order
            , fk_customer
            , fk_credit_card
            , fk_product
            , fk_territory
            , fk_date
            , order_date
            , due_date
            , ship_date
            , order_quantity
            , unit_price
            , unit_price_discount
            , gross_total
            , net_total
            , discount_amount
            , freight
            , tax
            , sub_total
            , total_due
            , sales_type
            , order_status
            , sales_reason_agg
            , had_discount
        from joined
    )

select *
from metrics