with
    int_sales as (
        select *
        from {{ ref('int_sales__metrics') }}
    )
    , reason as (
        select *
        from {{ ref('int_reason__aggregated') }}
    )
    , customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    , product as (
        select *
        from {{ ref('dim_product') }}
    )
    , territory as (
        select *
        from {{ ref('dim_territory') }}
    )
    , credit_card as (
        select *
        from {{ ref('dim_credit_card') }}
    )
    , dates as (
        select *
        from {{ ref('dim_date') }}
    )
    , joined as (
        select
            int_sales.sk_sales_order
            , product.sk_product
            , customer.sk_customer
            , territory.sk_territory
            , credit_card.sk_credit_card
            , d1.sk_date as sk_order_date
            , d2.sk_date as sk_ship_date
            , d3.sk_date as sk_due_date
            , int_sales.sales_order_id
            , int_sales.sales_order_detail_id
            , int_sales.order_status
            , int_sales.online_order
            , int_sales.freight
            , int_sales.tax
            , int_sales.sub_total
            , int_sales.total_due
            , int_sales.order_quantity
            , int_sales.unit_price
            , int_sales.unit_price_discount
            , int_sales.gross_total
            , int_sales.net_total
            , int_sales.discount_amount
            , int_sales.had_discount
            , reason.reasons
        from int_sales
        left join product on int_sales.product_id = product.product_id
        left join customer on int_sales.customer_id = customer.customer_id
        left join territory on int_sales.territory_id = territory.territory_id
        left join credit_card on int_sales.credit_card_id = credit_card.credit_card_id
        left join reason on int_sales.sales_order_id = reason.sales_order_id
        left join dates d1 on int_sales.order_date = d1.date_day
        left join dates d2 on int_sales.ship_date = d2.date_day
        left join dates d3 on int_sales.due_date = d3.date_day
    )

select *
from joined