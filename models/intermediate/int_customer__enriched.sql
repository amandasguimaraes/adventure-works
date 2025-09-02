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
        , person.business_entity_id as person_business_entity_id
        , person.person_type
        , person.full_name
        , store.business_entity_id as store_business_entity_id
        , store.store_name
        , case 
            when customer.store_id is not null then 'Company'
            when customer.person_id is not null then 'Individual'
            else 'Other'
        end as customer_category
        , case 
            when customer.person_id is null and customer.store_id is not null then 'B2B'
            when customer.person_id is not null and customer.store_id is null then 'B2C'
            when customer.person_id is not null and customer.store_id is not null then 'B2B Contact'
            else 'Other'
        end as customer_type
    from customer
    left join person on customer.person_id = person.business_entity_id
    left join store on customer.store_id = store.business_entity_id
)
    
select *
from joined