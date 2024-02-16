with 
sales as 
(
    select 
    *
    from {{ ref('int_added_first_purchase_date') }}
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

store_info as (
    select 
        --joining key
        store_id,

        --information to include
        is_store_hidden,
        store_short_name,
        store_post_code,
        store_prefecture,
        store_city,
        store_combined_address
    from {{ ref('int_address_joined_to_store') }}
),

 int_joined_sales_emolyee_customer_store_info as (
    select *
    from sales
    left join employee_info using (employee_id)
    left join store_info using (store_id)
    )

select 
*
from int_joined_sales_emolyee_customer_store_info