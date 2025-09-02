with 
    int_sales as ( 
        select 
            {{ dbt_utils.generate_surrogate_key(['sales_order_id', 'sales_order_detail_id']) }} as sk_sales_order 
            , *
        from {{ ref('int_sales__metrics') }} 
    ) 
    , dim_date as (
        select *
        from {{ ref('dim_date') }}
    )
    , dim_credit_card as (
        select *
        from {{ ref('dim_credit_card') }}
    )
    , dim_customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    , dim_territory as (
        select *
        from {{ ref('dim_territory') }}
    )
    , dim_product as (
        select *
        from {{ ref('dim_product') }}
    )
    , dim_reason as (
        select *
        from {{ ref('dim_reason') }}
    )
    , joined_sales as (
        select
            int_sales.sk_sales_order
            , dim_date.sk_date as fk_date
            , dim_credit_card.sk_credit_card as fk_credit_card
            , dim_customer.sk_customer as fk_customer
            , dim_territory.sk_territory as fk_territory
            , dim_product.sk_product as fk_product
            , dim_reason.sk_reason as fk_reason
            , int_sales.ship_to_address_id
            , int_sales.sales_order_id
            , int_sales.order_status 
            , int_sales.sales_type
            , int_sales.order_quantity
            , int_sales.unit_price
            , int_sales.unit_price_discount
            , int_sales.gross_total
            , int_sales.net_total
            , int_sales.prorated_freight
            , int_sales.prorated_tax
            , int_sales.discount_amount
            , int_sales.had_discount
            , int_sales.line_total
            , int_sales.order_date
            , int_sales.due_date
            , int_sales.ship_date
        from int_sales
        left join dim_date on int_sales.order_date = dim_date.date_day
        left join dim_credit_card on int_sales.credit_card_id = dim_credit_card.credit_card_id
        left join dim_customer on int_sales.customer_id = dim_customer.customer_id
        left join dim_territory on int_sales.bill_to_address_id = dim_territory.address_id
        left join dim_product on int_sales.product_id = dim_product.product_id
        left join dim_reason on int_sales.sales_order_id = dim_reason.sales_order_id
    )

select *
from joined_sales
