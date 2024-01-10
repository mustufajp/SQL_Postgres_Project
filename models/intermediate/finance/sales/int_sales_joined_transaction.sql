with
sales as (
    select 
        sales_id,
        transaction_id,
        sales_amount,
        sales_type,
        employee_id,
        sales_store_id,
        points_given,
        points_used,
        total_discount,
        customer_age,
        customer_category,
        customer_payment_type,
        customer_payment_method,
        sales_date,
        affiliate_commission_point,
        affiliate_commission_amount,
        custom_discount_price,
        custom_discount_coupon
    from {{ ref('stg_saad_shop__sales') }}

),

transaction as (
    select 
        transaction_id,
        transaction_real_world_amount,
        sender_id,
        receiver_id,
        transaction_type,
        parent_transaction_id
    from {{ ref('stg_saad_shop__transactions') }}
),

int_sales_joined_transaction as (
    select *
    from sales
    inner join transaction 
    USING (transaction_id)
)

select *
from int_sales_joined_transaction
