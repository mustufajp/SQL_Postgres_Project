with 
int_sales_aggregated_to_product as 
(
select *
from {{ ref('int_sales_aggregated_to_product') }}
),

customer_analysis_dashboard_aggregated_to_products as (

    select {{ dbt_utils.star(from=ref('int_sales_aggregated_to_product'), except=[
    "key",
    "sku_id",
    "product_custom_discount_price",
    "product_custom_discount_coupon",
    "custom_point_back_rate",
    "point_back_rate",
    "product_point_spent_per",
    "product_point_back_amount"
    ]) }}
    from int_sales_aggregated_to_product
)

select * from customer_analysis_dashboard_aggregated_to_products