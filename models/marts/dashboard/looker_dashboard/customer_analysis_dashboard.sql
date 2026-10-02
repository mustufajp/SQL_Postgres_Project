with 
customer_analysis_dashboard as 
(
    select 
    {{ dbt_utils.star(from=ref('int_added_membership_status_to_transaction'), except=[
        "store_id",
        "employee_id",
        "year_month",
        ]) }}

    from {{ ref('int_added_membership_status_to_transaction') }}
)

select 
*
from customer_analysis_dashboard
