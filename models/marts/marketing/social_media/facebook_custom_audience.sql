with
customer_list as (
    select *
    from {{ ref('customer_analysis_dashboard_aggregated_to_customer') }}
    order by customer_created_at desc
),
    facebook_custom_audience as (
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
        from customer_list
    )

select
*
from facebook_custom_audience
-- where date_trunc('day',customer_created_at) > '2024-08-04'
