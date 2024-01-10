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
*
from int_refunds_removed_from_sales_transaction
