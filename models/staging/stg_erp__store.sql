with
    source_store as (
        select *            
        from {{ source('erp','sales_store') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as store_id
            , cast(name as string) as store_name
        from source_store
    )

select *
from renamed