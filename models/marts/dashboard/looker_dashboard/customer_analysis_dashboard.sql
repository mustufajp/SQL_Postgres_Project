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
    DATE_PART('day', CURRENT_DATE AT TIME ZONE 'Asia/Tokyo'-last_purchase) as days_last_purchase,
    CAST(
        EXTRACT(
            epoch FROM 
            (CASE
                WHEN DATE_TRUNC('year', CURRENT_DATE) + (DATE_PART('doy', customer_date_of_birth) - 1) * INTERVAL '1 day' >= CURRENT_DATE
                THEN DATE_TRUNC('year', CURRENT_DATE) + (DATE_PART('doy', customer_date_of_birth) - 1) * INTERVAL '1 day' - CURRENT_DATE
                ELSE DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year' + (DATE_PART('doy', customer_date_of_birth) - 1) * INTERVAL '1 day' - CURRENT_DATE
            END)
        ) / 86400 AS INTEGER
    ) AS days_until_birthday,
    case when customer_id is null then 0
    else 1 end as member_status
    from {{ ref('int_churn_analysis_added_to_sales') }}
)

select 
*
from customer_analysis_dashboard