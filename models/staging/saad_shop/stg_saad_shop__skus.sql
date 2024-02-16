with 

source as (

    select * from {{ source('saad_shop', 'skus') }}

),

renamed as (

    select
        id as sku_id,
        product_id,
        saad_sku_id,
        sku_code,
        price as current_price,
        total_inventory,
        created_at as product_inception_at,
        updated_at as product_updated_at,
        category as product_category,
        code as product_code,
        type as product_type,
        size as product_size,
        concat(t.product_category,t.product_code,product_type) as sku_code_na_size


    from source

)

select * from renamed
