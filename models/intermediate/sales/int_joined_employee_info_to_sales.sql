with
    int_joined_location_to_sales as (
        select * from {{ ref("int_joined_location_to_sales") }}
    ),
    stg_saad_shop__user_profiles as (
        select * from {{ ref("stg_saad_shop__user_profiles") }}
    ),

    int_joined_employee_info_to_sales as (
        select
            c.*, p.first_name as employee_first_name, p.last_name as employee_last_name
        from int_joined_location_to_sales as c
        left join stg_saad_shop__user_profiles as p on p.user_id = c.employee_id
    )

select *
from int_joined_employee_info_to_sales