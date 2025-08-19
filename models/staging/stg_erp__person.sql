with
    source_person as (
        select *            
        from {{ source('erp','person_person') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as business_entity_id
            , cast(persontype as string) as person_type
            , cast(firstname as string) as first_name
            , cast(middlename as string) as middle_name
            , cast(lastname as string) as last_name
            , trim(concat(firstname, ' ', coalesce(middlename, ''), ' ', lastname)) as full_name
        from source_person
    )

select *
from renamed