with 
sales as 
(
    select 
    {{ dbt_utils.star(from=ref('int_added_churn_rate'), except=["year_month"]) }}
    from {{ ref('int_added_churn_rate') }}
),

employee_info as (
    select
    --joining key
    employee_id,
    --info to include
    employee_type,
    employee_status,
    employee_first_name,
    employee_last_name
    from {{ ref('int_add_info_to_employee') }}
),

customer_info as (
    select
    --joining key
    customer_id,
    --info to include
    customer_phone_number,
    is_customer_email_verified,
    customer_email,
    is_magazine_subscribed,
    is_agree_to_term_of_service,
    customer_first_name,
    customer_last_name,
    customer_gender,
    customer_date_of_birth,
    customer_post_code,
    customer_prefecture,
    customer_city,
    customer_combined_address
    from {{ ref('int_add_addresses_to_user_info') }}
),

store_info as (
    select 
        --joining key
        store_id,

        --information to include
        store_short_name,
        store_post_code,
        store_prefecture,
        store_city,
        store_combined_address
    from {{ ref('int_address_joined_to_store') }}
),

customer_analysis_dashboard as (
    select *
    from sales
    left join customer_info using (customer_id)
    left join employee_info using (employee_id)
    left join store_info using (store_id)
    )

select 
*
from customer_analysis_dashboard
