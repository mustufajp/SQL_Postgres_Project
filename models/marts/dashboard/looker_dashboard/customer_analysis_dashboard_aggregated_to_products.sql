with 
int_sales_aggregated_to_product as 
(
select *
from {{ ref('int_sales_joined_to_product') }}
),

customer_analysis_dashboard_aggregated_to_products as (

    select 
    *
    from int_sales_aggregated_to_product
)

select * from customer_analysis_dashboard_aggregated_to_products

