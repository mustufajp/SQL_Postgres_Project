with 

product as (
    select 
    t.*,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM {{ ref('int_skus_joined_to_sku_transaction') }} sub
            WHERE sub.transaction_id = t.transaction_id
            AND sub.product_category ILIKE '%box%'
        ) THEN 'ギフト'
        ELSE '非ギフト'
    END AS is_gift
    from {{ ref('int_skus_joined_to_sku_transaction') }} t
),
sales as (
    select 
     {{ dbt_utils.star(from=ref('customer_analysis_dashboard'), except=[
        "sales_amount",
        "points_used",
        "total_discount",
        "product_quantity",
        "product_quantity_sold"
        ]) }}

    from {{ ref('customer_analysis_dashboard') }}
    ),

int_sales_aggregated_to_product as (
    select *
    ,case when sales_type='refund' then -1*((product_quantity*price_at_purchase)-(product_quantity*product_discounted_amount)) else (product_quantity*price_at_purchase)-(product_quantity*product_discounted_amount)end as product_sales_amount
    from product
    left join sales 
    using (transaction_id)

)

select *
from int_sales_aggregated_to_product