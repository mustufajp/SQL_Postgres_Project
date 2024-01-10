with
    sku_transactions as (select * from {{ ref("stg_saad_shop__sku_transactions") }}),
    skus as (select * from {{ ref("stg_saad_shop__skus") }}),
    int_sales_aggregated_to_product as (
        select *
        from sku_transactions
        left join skus using (sku_id)
        order by transaction_id
    )

select * FROM int_sales_aggregated_to_product