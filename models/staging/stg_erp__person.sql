with
    source_person as (
        select *            
        from {{ source('erp','person_person') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as business_entity_id
            , case
                when cast(persontype as string)  = 'SC' then 'Store Contact' 
                when cast(persontype as string)  = 'IN' then 'Individual Client' 
                when cast(persontype as string)  = 'SP' then 'Sales Person'
                when cast(persontype as string)  = 'EM' then 'Employee' 
                when cast(persontype as string)  = 'VC' then 'Vendor Contact'
                when cast(persontype as string)  = 'GC' then 'General Contact'
                else persontype
            end as person_type
            , cast(firstname as string) as first_name
            , cast(middlename as string) as middle_name
            , cast(lastname as string) as last_name
            , TRIM(
                coalesce(first_name, '') || ' ' ||
                coalesce(middle_name || ' ', '') ||
                coalesce(last_name, '')
            ) AS full_name
        from source_person
    )

select *
from renamed