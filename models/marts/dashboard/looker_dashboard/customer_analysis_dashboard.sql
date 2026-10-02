with 
customer_analysis_dashboard as 
(
    select 
    {{ dbt_utils.star(from=ref('int_joined_sales_emolyee_customer_store_info'), except=[
        "store_id",
        "employee_id",
        "year_month",
        ]) }}

    from {{ ref('int_joined_sales_emolyee_customer_store_info') }}
)

select 
*
from customer_analysis_dashboard
