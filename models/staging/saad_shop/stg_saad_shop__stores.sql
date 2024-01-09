with 

source as (

    select * from {{ source('saad_shop', 'stores') }}

),

renamed as (

    select
        id as store_id
        ,name as store_name
        ,address_id as store_address_id

    from source

)

select * from renamed
