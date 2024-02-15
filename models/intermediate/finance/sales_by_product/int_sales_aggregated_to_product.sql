with 

product as (
    select 
    *
    from {{ ref('int_skus_joined_to_sku_transaction') }} 
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