with 
sales as 
(
    select *
    from {{ ref('int_refunds_removed_from_sales_transaction') }}
),

employee_info as (
    select
    employee_id,
    employee_type,
    employee_status
    from {{ ref('int_add_info_to_employee') }}
),

customer_info as (
    select
    customer_id,
    user_phone_number
    
    
    from {{ ref('int_add_addresses_to_user_info') }}
),

store_info as (
    select *
    from {{ ref('int_address_joined_to_store') }}
),

customer_analysis_dashboard as (
    select *
    from sales
    left join customer_info using (customer_id)
    left join employee_info using (employee_id)
    left join store_info on sales.sales_store_id =store_info.store_id 
    )

select 
*
from customer_analysis_dashboard
