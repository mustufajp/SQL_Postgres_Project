with 

sales_by_customer as (
    select 
    *
    from {{ ref('int_sales_aggregated_to_customer') }}
),

customer_analysis_dashboard_aggregated_to_customer as (
    select 
    *
    from sales_by_customer
)

SELECT 
*
FROM 
customer_analysis_dashboard_aggregated_to_customer