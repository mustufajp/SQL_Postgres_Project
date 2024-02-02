With customer_info as (
    select
    --joining key
    customer_id,
    --info to include
    customer_phone_number,
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
        sum(points_given) as points_given,
        sum(points_used) as points_used,
        sum(total_discount) as total_discount,
        sum(affiliate_commission_point) as affiliate_commission_point,
        sum(affiliate_commission_amount) as affiliate_commission_amount,
        sum(custom_discount_price)as custom_discount_price,
        sum(custom_discount_coupon) as custom_discount_coupon,
        sum(product_quantity_sold)as product_quantity_sold,
        sum(transaction_real_world_amount) as transaction_real_world_amount,
        sum(refund_amount) as refund_amount,
        sum(refund_quantity) as refund_quantity,
        max(customer_category) as customer_category,
        max(customer_age) as est_customer_age,
        first_purchase,
        last_purchase,
        sales_counts
    from {{ ref('int_added_first_purchase_date') }}
    group by customer_id,
    first_purchase,
    last_purchase,
    sales_counts
),
customer_analysis_dashboard_aggregated_to_customer as (
    select
    *
    from customer_info
    left join sales 
    USING (customer_id) 
)

select 
*
from customer_analysis_dashboard_aggregated_to_customer
