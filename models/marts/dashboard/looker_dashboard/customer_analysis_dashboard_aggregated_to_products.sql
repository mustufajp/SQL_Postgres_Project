with 

product as (
    select 
    *
    from {{ ref('int_sales_aggregated_to_product') }}
),
sales as (
    select 
     {{ dbt_utils.star(from=ref('customer_analysis_dashboard'), except=[
        "sales_amount",
        "points_used",
        "total_discount"
        ]) }}

    from {{ ref('customer_analysis_dashboard') }}
    ),

customer_analysis_dashboard_aggregated_to_products as (
    select *
    from product
    left join sales 
    using (transaction_id)

)

select *
from customer_analysis_dashboard_aggregated_to_products
