with
    sku_transactions as (
        select
        transaction_id,
        sku_id,
        product_quantity,
        price_at_purchase,
        product_discounted_amount,
        sku_transaction_id
        from {{ ref("stg_saad_shop__sku_transactions") }}
        ),
    skus as (
        select 
        sku_id,
        sku_code,
        current_price,
        total_inventory,
        product_inception_at,
        product_updated_at,
        product_category,
        product_code,
        product_type,
        product_size
        from {{ ref("stg_saad_shop__skus") }}
        ),
    int_sales_aggregated_to_product as (
        select *
        ,(product_quantity*price_at_purchase)-(product_quantity*product_discounted_amount) as product_sales_amount
        from sku_transactions
        left join skus using (sku_id)
        order by transaction_id
    )

select * FROM int_sales_aggregated_to_product

