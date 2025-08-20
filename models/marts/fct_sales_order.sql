with
    int_sales as (
        select *
        from {{ ref('int_sales__metrics') }}
    )
    , dim_date as (
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
        from {{ ref('dim_reason') }}
    )
    , joined as (
        select
            int_sales.sk_sales_order
            , dim_date.sk_date as fk_date
            , credit_card.sk_credit_card as fk_credit_card
            , customer.sk_customer as fk_customer
            , territory.sk_territory as fk_territory
            , product.sk_product as fk_product
            , reason.sk_reason as fk_reason
            , int_sales.order_date
            , int_sales.due_date
            , int_sales.ship_date
            , int_sales.sales_order_id
            , int_sales.order_status 
            , int_sales.order_quantity
            , int_sales.unit_price
            , int_sales.unit_price_discount
            , int_sales.had_discount
            , int_sales.discount_amount
            , int_sales.gross_total
            , int_sales.net_total
        from int_sales
        left join dim_date on int_sales.order_date = dim_date.date_day
        left join credit_card on int_sales.credit_card_id = credit_card.credit_card_id
        left join customer on int_sales.customer_id = customer.customer_id
        left join territory on int_sales.address_id = territory.address_id
        left join product on int_sales.product_id = product.product_id
        left join reason on int_sales.sales_order_id = reason.sales_order_id
    )

select *
from joined