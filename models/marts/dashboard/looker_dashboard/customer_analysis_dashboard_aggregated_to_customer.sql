With customer_info as (
    select
    --joining key
    customer_id,
    --info to include
    customer_phone_number,
    customer_created_at,
    is_customer_email_verified,
    customer_email,
    is_magazine_subscribed,
    is_agree_to_term_of_service,
    customer_first_name,
    customer_last_name,
    customer_gender,
    customer_date_of_birth,
    customer_post_code,
    customer_prefecture,
    customer_city,
    customer_combined_address
    from {{ ref('int_add_addresses_to_user_info') }}
),

sales as (

     select 
        customer_id,
        sum(sales_amount) as sales_amount,
        avg(sales_amount) as avg_sales_amount,
        sum(points_given) as points_given,
        sum(points_used) as points_used,
        sum(total_discount) as total_discount,
        sum(product_quantity_sold)as product_quantity,
        max(customer_category) as customer_category,
        max(customer_age) as est_customer_age,
        COUNT(transaction_id) as sales_counts,
        first_purchase,
        last_purchase
    from {{ ref('int_joined_sales_emolyee_customer_store_info') }}
    group by customer_id,
    first_purchase,
    last_purchase
),
customer_analysis_dashboard_aggregated_to_customer as (
    select
    *,
    case 
    when sales_counts is null then '非購入会員'
    when sales_counts is not null then '購入会員'
    end as user_type,
  CAST(
        EXTRACT(
            epoch FROM 
            (CASE
                WHEN DATE_TRUNC('year', CURRENT_DATE) + (DATE_PART('doy', customer_date_of_birth) - 1) * INTERVAL '1 day' >= CURRENT_DATE
                THEN DATE_TRUNC('year', CURRENT_DATE) + (DATE_PART('doy', customer_date_of_birth) - 1) * INTERVAL '1 day' - CURRENT_DATE
                ELSE DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year' + (DATE_PART('doy', customer_date_of_birth) - 1) * INTERVAL '1 day' - CURRENT_DATE
            END)
        ) / 86400 AS INTEGER
    ) AS days_until_birthday
    from customer_info
    left join sales 
    USING (customer_id) 
)

select 
*
from customer_analysis_dashboard_aggregated_to_customer