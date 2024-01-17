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
        ]) }},
    DATE_PART('day', CURRENT_DATE AT TIME ZONE 'Asia/Tokyo'-sales_date) as days_last_purchase,
    CASE 
        WHEN EXTRACT(DOY FROM customer_date_of_birth) >= EXTRACT(DOY FROM CURRENT_DATE AT TIME ZONE 'Asia/Tokyo')
        THEN EXTRACT(DOY FROM customer_date_of_birth) - EXTRACT(DOY FROM CURRENT_DATE AT TIME ZONE 'Asia/Tokyo')
        ELSE EXTRACT(DOY FROM customer_date_of_birth + INTERVAL '1 year') - EXTRACT(DOY FROM CURRENT_DATE AT TIME ZONE 'Asia/Tokyo')
    END AS days_until_birthday
    from {{ ref('int_churn_analysis_added_to_sales') }}
)

select 
*
from customer_analysis_dashboard
