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
    int_skus_joined_to_sku_transaction as (
        select *
        from sku_transactions
        left join skus using (sku_id)
        order by transaction_id
    )

select t.* 
    ,CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM int_skus_joined_to_sku_transaction sub
            WHERE sub.transaction_id = t.transaction_id
            AND sub.product_category ILIKE '%box%'
        ) THEN 'ギフト'
        ELSE '非ギフト'
    END AS is_gift

FROM int_skus_joined_to_sku_transaction t

