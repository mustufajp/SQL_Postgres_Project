with 

product as (
    select 
    *
    from {{ ref('int_sales_aggregated_to_product') }}
),

customer_analysis_dashboard_aggregated_to_products as (
    select 
    t.*,
    concat(t.product_category,t.product_code,product_type) as sku_code_na_size
    from product t
)

SELECT 
*
FROM 
customer_analysis_dashboard_aggregated_to_products
