with 

user_info as (

    select
        user_id as customer_id,
        address_id,

        user_phone_number as customer_phone_number,
        user_status as customer_status,
        user_type as customer_type,
        is_user_email_verified as is_customer_email_verified,
        user_email as customer_email,
        is_user_verified as is_customer_verified,
        user_kyc_status as customer_kyc_status,
        user_created_at as customer_created_at,
        user_updated_at as customer_updated_at,
        is_magazine_subscribed,
        is_agree_to_term_of_service,
        is_user_profile_verified as is_customer_profile_verified,
        first_name as customer_first_name,
        last_name as customer_last_name,
        gender as customer_gender,
        date_of_birth as customer_date_of_birth,
        user_profile_updated_at as customer_profile_updated_at
    from {{ ref('int_user_profile_joined_to_users') }}
    where user_type='customer'
),
addresses as (
    select
        address_id,
        post_code as customer_post_code,
        prefecture as customer_prefecture ,
        city as customer_city,
        town as customer_town,
        line_one as customer_line_one,
        line_two as customer_line_two,
        latitude as customer_latitude,
        longitude as customer_longitude,
        country as customer_country,
        combined_address as customer_combined_address
    from {{ ref('stg_saad_shop__addresses') }}
),

int_add_addresses_to_user_info as (
    select *
    from user_info
    left join addresses
    using (address_id)
)

select 
*
from int_add_addresses_to_user_info