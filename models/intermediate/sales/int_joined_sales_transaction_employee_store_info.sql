with
    sales as (select * from {{ ref("stg_saad_shop__sales") }}),
    transactions as (select * from {{ ref("stg_saad_shop__transactions") }}),
    employee_users as (select * from {{ ref("stg_saad_shop__employee_users") }}),
    stores as (select * from {{ ref("stg_saad_shop__stores") }}),

    int_joined_sales_transaction_employee_store_info as (
        select *
        from sales as s
        join transactions as t using (transaction_id)
        left join employee_users as e using (employee_id)
        left join stores as store on sales_store_id = store_id
    )

select
employee_id
transaction_id,
sales_id,
sales_amount,
sales_type,
points_given,
points_used,
total_discount,
customer_age,
customer_gender,
customer_payment_type,
customer_payment_method,
sales_date,
affiliate_commission_point,
affiliate_commission_amount,
custom_discount_price,
custom_discount_coupon,
customer_id,
parent_transaction_id,
employee_type,
employee_status,
store_name,
store_address_id,
transaction_type
from int_joined_sales_transaction_employee_store_info
