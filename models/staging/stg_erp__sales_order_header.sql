with
    source_sales_order_header as (
        select *            
        from {{ source('erp','sales_salesorderheader') }}
    )
    , renamed as (
        select
            cast(salesorderid as int) as sales_order_id	
            , cast(salespersonid as int) as sales_person_id
            , cast(billtoaddressid as int) as bill_to_address_id	
            , cast(customerid as int) as customer_id
            , cast(creditcardid as int) as credit_card_id
            , cast(territoryid as int) as territory_id
            , cast(shiptoaddressid as int) as ship_to_address_id
            , case
                when cast(status as int) = 1 then 'In Process'
                when cast(status as int) = 2 then 'Approved'
                when cast(status as int) = 3 then 'Backordered'
                when cast(status as int) = 4 then 'Rejected'
                when cast(status as int) = 5 then 'Shipped'
                when cast(status as int) = 6 then 'Cancelled'
            end as order_status
            , case
                when cast(onlineorderflag as boolean) = true then 'Online'
                when cast(onlineorderflag as boolean) = false then 'Physical'
            end as sales_type
            , cast(subtotal as numeric(18,4)) as sub_total  
            , cast(taxamt as numeric(18,4)) as tax 
            , cast(freight as numeric(18,4)) as freight  
            , cast(totaldue as numeric(18,4)) as total_due
            , cast(orderdate as date) as order_date
            , cast(duedate as date) as due_date
            , cast(shipdate as date) as ship_date
        from source_sales_order_header
    )

select *
from renamed