with
    staging_users as (select * from {{ ref("stg_saad_shop__users") }}),

    stg_saad_shop__user_profiles as (
        select * from {{ ref("stg_saad_shop__user_profiles") }}
    ),

    stg_saad_shop__addresse as (select * from {{ ref("stg_saad_shop__addresses") }}),

    stg_saad_shop__employee_users as (
        select * from {{ ref("stg_saad_shop__employee_users") }}
    ),

    int_fb_custom_audience_list as (
        select *
        from staging_users
        left join stg_saad_shop__user_profiles using (user_id)
        left join stg_saad_shop__addresse using (user_id)
        left join stg_saad_shop__employee_users on user_id = employee_id
    )

select
    customer_email as email,
    concat('81', substring(cast(customer_phone_number as varchar), 2)) as phone_number,
    first_name as fn,
    last_name as ln,
    'jp' as country,
    post_code as zip
from int_fb_custom_audience_list
where employee_id is null
