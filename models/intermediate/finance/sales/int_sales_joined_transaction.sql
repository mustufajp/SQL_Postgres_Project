with
    sales as (
        select
            {{
                dbt_utils.star(
                    from=ref("stg_saad_shop__sales"),
                    except=[
                        "affiliate_commission_point",
                        "affiliate_commission_amount",
                        "custom_discount_price",
                        "custom_discount_coupon",
                        "custom_point_back",
                        "customer_payment_type",
                        "customer_payment_method",
                    ],
                )
            }}
        from {{ ref("stg_saad_shop__sales") }}

    ),

    transaction as (
        select transaction_id, sender_id, receiver_id, parent_transaction_id
        from {{ ref("stg_saad_shop__transactions") }}
    ),
    sku_transaction as (
        select transaction_id, sum(product_quantity) as product_quantity
        from {{ ref("stg_saad_shop__sku_transactions") }}
        group by transaction_id
    ),

    int_sales_joined_transaction as (
        select
            *,
            case
                when sales_type = 'refund' then receiver_id else sender_id
            end as customer_id
        from sales
        inner join transaction using (transaction_id)
        left join sku_transaction using (transaction_id)
    )

select *
from int_sales_joined_transaction
