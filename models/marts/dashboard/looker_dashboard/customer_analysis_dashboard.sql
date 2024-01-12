with 
customer_analysis_dashboard as 
(
    select 
    {{ dbt_utils.star(from=ref('int_joined_sales_emolyee_customer_store_info'), except=[
        "store_id",
        "employee_id",
        "sales_id",
        "affiliate_commission_point",
        "affiliate_commission_amount",
        "custom_discount_price",
        "custom_discount_coupon",
        "transaction_real_world_amount",
        "transaction_type",
        "points_given"
        
        ]) }}
    from {{ ref('int_joined_sales_emolyee_customer_store_info') }}
),

select 
*
from customer_analysis_dashboard
