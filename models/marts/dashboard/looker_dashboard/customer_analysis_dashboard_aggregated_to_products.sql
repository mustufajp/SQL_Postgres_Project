with 

sales_by_product as (
    select 
    *
    from {{ ref('int_sales_aggregated_to_product') }}
),

customer_analysis_dashboard_aggregated_to_products as (
    select 
    *
    from sales_by_product
)

SELECT 
*
FROM 
customer_analysis_dashboard_aggregated_to_products
