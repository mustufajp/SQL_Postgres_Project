with 

user_info as (

    select
        user_id as customer_id,
        user_phone_number,
        user_status,
        user_type,
        is_user_email_verified,
        user_email,
        is_user_verified,
        user_kyc_status,
        user_created_at,
        user_updated_at,
        is_magazine_subscribed,
        is_agree_to_term_of_service,
        is_user_profile_verified,
        first_name,
        last_name,
        gender,
        date_of_birth,
        address_id,
        user_profile_updated_at
    from {{ ref('int_user_profile_joined_to_users') }}
    where user_type='customer'
),
addresses as (
    select
        address_id,
        post_code,
        prefecture,
        city,
        town,
        line_one,
        line_two,
        full_width_kana,
        latitude,
        longitude,
        country,
        combined_address
    from {{ ref('stg_saad_shop__addresses') }}
),

int_add_addresses_to_user_info as (
    select *
    from user_info
    left join addresses
    using (address_id)
)

select * from int_add_addresses_to_user_info

