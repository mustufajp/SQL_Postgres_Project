with 
sales as (
    select 
    *
    from {{ ref('int_sales_joined_transaction') }}
),

refund as (
    select 
    transaction_id,
    sales_type,
    parent_transaction_id
    FROM {{ ref('int_sales_joined_transaction') }}
    where sales_type = 'refund'
),
int_refunds_removed_from_sales_transaction as 
(
    select 
    sales.*
    ,CASE WHEN refund.sales_type = 'refund' THEN 1 ELSE 0 END AS is_refund
    from sales
    left join refund 
    ON refund.parent_transaction_id =sales.transaction_id
    WHERE sales.sales_type !='refund'
)

SELECT 
--date
sales_date,

--id
transaction_id,
sales_id,
sender_id as customer_id,
employee_id,
sales_store_id,

--transaction detail
case when is_refund=0 then sales_amount else 0 end as sales_amount,
case when is_refund=0 then points_given else 0 end as points_given,
case when is_refund=0 then points_used else 0 end as points_used,
case when is_refund=0 then total_discount else 0 end as total_discount,
case when is_refund=0 then affiliate_commission_point else 0 end as affiliate_commission_point,
case when is_refund=0 then affiliate_commission_amount else 0 end as affiliate_commission_amount,
case when is_refund=0 then custom_discount_price else 0 end as custom_discount_price,
case when is_refund=0 then custom_discount_coupon else 0 end as custom_discount_coupon,
transaction_real_world_amount,
transaction_type,
is_refund,

--customer info
customer_age,
customer_category,
customer_payment_type,
customer_payment_method

from int_refunds_removed_from_sales_transaction
