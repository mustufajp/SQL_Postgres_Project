with 

source as (

    select * from {{ source('saad_shop', 'user_profiles') }}

),

renamed as (

    select
        verified as is_user_profile_verified,
        user_id,
        first_name,
        last_name,
        kana_first_name,
        kana_last_name,
        gender,
        cast (date_of_birth as date) as date_of_birth,
        address_id,
        updated_at as user_profile_updated_at
    from source

)

select * from renamed
