with

sales as (

    select *
    from {{ ref('int_sales_joined_transaction') }}
),

int_added_first_purchase_date as 
(
select 
*,
MIN(sales_date) OVER (PARTITION BY customer_id) AS first_purchase,
MAX(sales_date) OVER (PARTITION BY customer_id) AS last_purchase,
LEAD(date(sales_date)) OVER (PARTITION BY customer_id order by sales_date )-date(sales_date) AS days_to_next_purchase,
COUNT(transaction_id) OVER (PARTITION BY customer_id) as sales_counts
from sales
order by customer_id
)

select *
from int_added_first_purchase_date