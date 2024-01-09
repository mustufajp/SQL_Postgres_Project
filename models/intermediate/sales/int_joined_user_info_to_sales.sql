with
    staging_users as (select * from {{ ref("stg_saad_shop__users") }}),

    stg_saad_shop__user_profiles as (
        select * from {{ ref("stg_saad_shop__user_profiles") }}
    ),

    int_joined_employee_info_to_sales as (
        select * from {{ ref("int_joined_employee_info_to_sales") }}
    ),

    combined_user_info as (
        select *
        from staging_users
        left join stg_saad_shop__user_profiles using (user_id)
    ),

    int_joined_user_info_to_sales as (
        select *
        from int_joined_employee_info_to_sales
        left join combined_user_info on customer_id = user_id
    )

select *
from int_joined_user_info_to_sales
