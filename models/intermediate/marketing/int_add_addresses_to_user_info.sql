with 

user_info as (

    select *
    from {{ ref('int_user_profile_joined_to_users') }}
    where user_type='customer'
),
addresses as (
    select
    *
    from {{ ref('stg_saad_shop__addresses') }}
),

int_add_addresses_to_user_info as (
    select *
    from user_info
    left join addresses
    using (address_id)
)

select * from int_add_addresses_to_user_info