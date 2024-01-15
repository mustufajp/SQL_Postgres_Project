with 

product as (
    select *
    from {{ ref('int_sales_aggregated_to_product') }}
),
sales as 