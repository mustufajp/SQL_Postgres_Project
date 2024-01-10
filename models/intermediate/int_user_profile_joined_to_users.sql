With 

users as (
    select 
        user_id,
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
        is_agree_to_term_of_service
    from {{ ref('stg_saad_shop__users') }}
),

user_profiles as (
    select
        is_user_profile_verified,
        user_id,
        first_name,
        last_name,
        gender,
        date_of_birth,
        address_id,
        user_profile_updated_at
    from {{ ref('stg_saad_shop__user_profiles') }}
),

int_user_profile_joined_to_users as (
    select *
    from users
    left join user_profiles
    using (user_id)
)

select * from int_user_profile_joined_to_users