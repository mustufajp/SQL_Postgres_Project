with 
customer_analysis_dashboard as 
(
    select 
    {{ dbt_utils.star(from=ref('int_churn_analysis_added_to_sales'), except=[
        "store_id",
        "employee_id",
        "sales_id",
        "points_given",
        "year_month",
        "sender_id",
        "receiver_id"
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
    case 
    when customer_id is not null and sales_date=first_purchase then '新規会員'
    when customer_id is not null then '既存会員'
    when customer_id is null then '非会員'
    end as member_status,
    case 
    when customer_id is not null and sales_date=first_purchase then 'New User'
    when customer_id is not null then 'Existing User'
    when customer_id is null then 'Non-User'
    end as member_status_en

    from {{ ref('int_churn_analysis_added_to_sales') }}
)

select 
*
from customer_analysis_dashboard
