with 

employee_users as (
    select
        employee_id,
        employee_store_id,
        employee_type,
        employee_status
    from {{ ref('stg_saad_shop__employee_users') }}
),

employee_info as (
    select
        user_id,
        user_phone_number,
        user_email,
        first_name,
        last_name,
        gender,
        date_of_birth,
        user_created_at,
        user_profile_updated_at
    from {{ ref('int_user_profile_joined_to_users') }}
    where user_type !='customer'
),

address_info as (
    select
        store_id,
        store_short_name,
        store_name,
        store_phone_number,
        store_combined_address
    from {{ ref('int_address_joined_to_store') }}
),

int_add_info_to_employee as (

    select *
    from employee_users
    left join employee_info on employee_id = user_id
    left join address_info on employee_store_id = store_id
)

select * from int_add_info_to_employee
