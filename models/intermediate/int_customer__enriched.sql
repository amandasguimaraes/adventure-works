with
    -- import models
    customer as (
        select *
        from {{ ref('stg_erp__customer') }}
    )
    ,  person as (
        select *
        from {{ ref('stg_erp__person') }}
    )
    ,  store as (
        select *
        from {{ ref('stg_erp__store') }}
    )
    -- transformation
    , joined as (
        select
            customer.customer_id
            , customer.person_id
            , customer.store_id
            , customer.territory_id
            , person.person_type
            , person.full_name
            , store.store_name
        from customer
        left join person on person.business_entity_id = customer.person_id
        left join store on store.business_entity_id = customer.store_id
    )
    
select *
from joined