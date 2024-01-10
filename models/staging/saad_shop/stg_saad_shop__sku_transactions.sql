with 

source as (

    select * from {{ source('saad_shop', 'sku_transactions') }}

),

renamed as (

    select
        transaction_id,
        custom_discount->>'coupon' AS product_custom_discount_coupon,
        custom_discount->>'price' AS product_custom_discount_price,
        sku_id,
        quantity,
        price_at_purchase,
        discounted_amount as product_discounted_amount,
        id as sku_transaction_id,
        point_back_amount as product_point_back_amount,
        point_spent_per as product_point_spent_per,
        custom_point_back_rate,
        point_back_rate

    from source

)

select * from renamed
