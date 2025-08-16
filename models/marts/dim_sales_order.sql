with
    int_sales as (
        select
             {{ dbt_utils.generate_surrogate_key(['sales_order_detail_id', 'sales_order_id']) }} as sk_sales_order
            , *
        from {{ ref('int_sales__metrics') }}
    )
    , date as (
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
    joined_fct as (
        select
            int_metrics.sk_sales_order
            , date.sk_date as fk_date
            , credit_card.sk_credit_card as fk_credit_card
            , customer.sk_customer as fk_customer
            , territory.sk_territory as fk_territory
            , product.sk_product as fk_product
            , reason.sk_reason as fk_reason
        from int_metrics
        left join date on int_metrics.order_date = date.date_day
        left join credit_card on int_metrics.credit_card_id = credit_card.credit_card_id
        left join customer on int_metrics.customerid = customer.customer_id
        left join territory on int_metrics.address_id = territory.address_id
        left join product on int_metrics.product_id = product.product_id
        left join reason on int_metrics.sales_order_id = reason.sales_order_id
    )
   
select *
from joined_fct