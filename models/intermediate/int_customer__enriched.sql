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
            , case 
                when store_id is not null then 'Company'
                when person_id is not null then 'Individual'
                else 'Other'
            end as customer_category
            , case 
                when person_id is null and store_id is not null then 'B2B'
                when person_id is not null and store_id is null then 'B2C'
                when person_id is not null and store_id is not null then 'B2B Contact'
                else 'Other'
            end as customer_type
        from customer
        left join person on person.business_entity_id = customer.person_id
        left join store on store.business_entity_id = customer.store_id
    )
    
select *
from joined