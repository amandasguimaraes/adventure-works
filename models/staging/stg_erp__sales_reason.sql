with
    source_sales_reason as (
        select *            
        from {{ source('erp','sales_salesreason') }}
    )
    , renamed as (
        select
            cast(salesreasonid as int) as sales_reason_id
            , cast(name as string) as reason_name
        from source_sales_reason
    )

select *
from renamed