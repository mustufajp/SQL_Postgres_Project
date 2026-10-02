with 

product as (
    select 
    *
    from {{ ref('int_skus_joined_to_sku_transaction') }} 
),
sales as (
    select 
     {{ dbt_utils.star(from=ref('int_added_membership_status_to_transaction'), except=[
        "sales_amount",
        "points_used",
        "total_discount",
        "product_quantity",
        "product_quantity_sold",
        "is_gift"
        ]) }}

    from {{ ref('int_added_membership_status_to_transaction') }}
    ),

int_sales_aggregated_to_product as (
    select *
    ,case when sales_type='refund' then -1*((product_quantity*price_at_purchase)-(product_quantity*product_discounted_amount)) else (product_quantity*price_at_purchase)-(product_quantity*product_discounted_amount)end as product_sales_amount
    ,case when sales_type='refund' then -1*(product_quantity*cost_unit_amount) else (product_quantity*cost_unit_amount)end as product_cost_amount
    from product
    left join sales 
    using (transaction_id)

)

select *
from int_sales_aggregated_to_product