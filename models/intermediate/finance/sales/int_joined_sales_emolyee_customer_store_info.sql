with 
sales as 
(
    select 
    *
    {{ ref('int_added_first_purchase_date') }}
),

employee_info as (
    select
       {{ dbt_utils.star(from=ref('int_add_info_to_employee'), except=[
        "employee_store_id",
        "employee_phone_number",
        "employee_email",
        "employee_gender",
        "employee_date_of_birth",
        "employee_profile_updated_at"
        ]) }}
    from {{ ref('int_add_info_to_employee') }}
),

store_info as (
    select 
         {{ dbt_utils.star(from=ref('int_address_joined_to_store'), except=[
        "store_name",
        "store_address_id",
        "store_phone_number",
        "store_created_at",
        "store_updated_at",
        "is_store_ecom_inv_hidden",
        "address_id",
        "store_post_code",
        "store_city",
        "store_town",
        "store_line_one",
        "store_line_two",
        "store_latitude",
        "store_longtitude",
        "store_country",
        "store_combined_address"
        ]) }}
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