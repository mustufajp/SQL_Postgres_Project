with 

source as (

    select * from {{ source('saad_shop', 'users') }}

),

renamed as (

    select
        id as user_id
        ,phone_number as customer_phone_number
        ,email as customer_email
        ,magazine as magazine_subs
        ,agree_to_tos as terms_of_service
    from source

)

select * from renamed
