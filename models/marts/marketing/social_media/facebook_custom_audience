with
    customer_list as (
        select  
         customer_email as email,
        concat('81', substring(cast(customer_phone_number as varchar), 2)) as phone_number,
        customer_first_name as fn,
        customer_last_name as ln,
        'jp' as country,
        customer_post_code as zip,
        customer_date_of_birth as dob,
        case when customer_gender= 'male' then 'M' when customer_gender='female' then 'F' else null end as gen,
        customer_created_at,
        customer_id
        from {{ ref('int_add_addresses_to_user_info') }}
    )

select
*
from customer_list
where date_trunc('day',customer_created_at) > '2024-03-31'
