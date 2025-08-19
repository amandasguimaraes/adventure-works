with
    source_sales_order_header as (
        select *            
        from {{ source('erp','sales_salesorderheader') }}
    )
    , renamed as (
        select
            cast(salesorderid as int) as sales_order_id		
            , cast(customerid as int) as customer_id
            , cast(creditcardid as int) as credit_card_id
            , cast(shiptoaddressid as int) as ship_to_address_id
            , case
                when cast(status as int) = 1 then 'in_process'
                when cast(status as int) = 2 then 'approved'
                when cast(status as int) = 3 then 'backordered'
                when cast(status as int) = 4 then 'rejected'
                when cast(status as int) = 5 then 'shipped'
                when cast(status as int) = 6 then 'cancelled'
            end as order_status
            , cast(onlineorderflag as boolean) as online_order
            , case
                when online_order = true then 'Online'
                when online_order = false then 'Physical'
            end as sales_type
            , cast(subtotal as numeric(18,4)) as sub_total  
            , cast(taxamt as numeric) as tax 
            , cast(freight as numeric) as freight  
            , cast(totaldue as numeric) as total_due
            , cast(orderdate as date) as order_date
            , cast(duedate as date) as due_date
            , cast(shipdate as date) as ship_date
        from source_sales_order_header
    )

select *
from renamed