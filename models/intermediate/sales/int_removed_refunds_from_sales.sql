WITH 
int_joined_sales_transaction_employee_store_info as 
(
    select *
    from {{ ref('int_joined_sales_transaction_employee_store_info') }}
),
refund as (
    select 
    transaction_id as refund_id
    FROM {{ ref('int_joined_sales_transaction_employee_store_info') }}
    where sales_type = 'refund'
),
int_removed_refunds_from_sales as 
(
    select 
    s.*
    ,CASE WHEN refund_id IS NULL THEN 0 ELSE 1 END AS is_refund
    from int_joined_sales_transaction_employee_store_info as s
    left join refund 
    ON s.parent_transaction_id =refund.refund_id
    WHERE s.sales_type !='refund'
)

SELECT * from int_removed_refunds_from_sales

