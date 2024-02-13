with 

product as (
    select 
    *
    from {{ ref('int_sales_aggregated_to_product') }}
),

customer_analysis_dashboard_aggregated_to_products as (
    select 
    t.*,
    concat(t.product_category,t.product_code,product_type) as sku_code_na_size,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM product sub
            WHERE sub.transaction_id = t.transaction_id
            AND sub.product_category ILIKE '%box%'
        ) THEN 'ギフト'
        ELSE '非ギフト'
    END AS is_gift
    from product t
)

SELECT 
*
FROM 
customer_analysis_dashboard_aggregated_to_products
