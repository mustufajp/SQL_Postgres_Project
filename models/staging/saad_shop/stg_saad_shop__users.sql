with 

source as (

    select * from {{ source('saad_shop', 'users') }}

),

renamed as (

    select
        id as user_id,
        password as user_password,
        username,
        phone_number as user_phone_number,
        avatar as user_avatar,
        status as user_status,
        type as user_type,
        email_verified as is_user_email_verified,
        email as user_email,
        verified as is_user_verified,
        stripe_customer_id,
        kyc_status as user_kyc_status,
        created_at as user_created_at,
        updated_at as user_updated_at,
        access as user_access,
        country as user_country,
        default_store_id as user_default_store_id,
        magazine as is_magazine_subscribed,
        agree_to_tos as is_agree_to_term_of_service

    from source
)

select * from renamed