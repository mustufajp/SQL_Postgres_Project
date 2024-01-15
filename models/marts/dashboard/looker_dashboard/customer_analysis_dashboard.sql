with 
customer_analysis_dashboard as 
(
    select 
    {{ dbt_utils.star(from=ref('int_churn_analysis_added_to_sales'), except=[
        "store_id",
        "employee_id",
        "sales_id",
        "affiliate_commission_point",
        "affiliate_commission_amount",
        "custom_discount_price",
        "custom_discount_coupon",
        "transaction_real_world_amount",
        "transaction_type",
        "points_given",
        "year_month"
        ]) }}
    from {{ ref('int_churn_analysis_added_to_sales') }}
)

select 
*
from customer_analysis_dashboard
