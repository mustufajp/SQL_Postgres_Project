with 

source as (

    select * from {{ source('saad_shop', 'user_profiles') }}

),

renamed as (

    select
        verified as status_verified,
        user_id,
        first_name,
        last_name,
        kana_first_name,
        kana_last_name,
        gender,
        date_of_birth,
        address_id,
        updated_at

    from source

)

select * from renamed
