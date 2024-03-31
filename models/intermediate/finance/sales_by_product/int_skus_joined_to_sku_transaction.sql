with
    sku_transactions as (
        select
         {{ dbt_utils.star(from=ref('stg_saad_shop__sku_transactions'), except=[
        "product_custom_discount_coupon",
        "product_custom_discount_price",
        "sku_transaction_key",
        "product_point_back_amount",
        "product_point_spent_per",
        "product_custom_point_back_rate",
        "product_point_back_rate"
        ]) }}
        from {{ ref("stg_saad_shop__sku_transactions") }}
        ),
    skus as (
        select 
        {{ dbt_utils.star(from=ref('stg_saad_shop__skus'), except=[
        "product_id",
        "saad_sku_id",
        "total_inventory",
        "product_code",
        "product_type",
        "product_size"
        ]) }}
        from {{ ref("stg_saad_shop__skus") }}
        ),
    cost as (
        select 
        sku_id,
        cost_unit_amount
        from {{ ref("stg_saad_shop__sku_cost_prices") }}
    ),
    int_skus_joined_to_sku_transaction as (
        select *
        from sku_transactions
        left join skus using (sku_id)
        left join cost using (sku_id)
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