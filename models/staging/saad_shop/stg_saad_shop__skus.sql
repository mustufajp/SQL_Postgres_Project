with 

source as (

    select * from {{ source('saad_shop', 'skus') }}

),

renamed as (

    select
        id as sku_id,
        product_id,
        upper(saad_sku_id) as saad_sku_id ,
        upper(sku_code) as sku_code,
        price as current_price,
        total_inventory,
        created_at as product_inception_at,
        updated_at as product_updated_at,
        upper(category) as product_category,
        upper(code) as product_code,
        upper(type) as product_type,
        upper(size) as product_size,
        upper(concat(category,code)) as category_code,
        upper(concat(category,code,type)) as sku_code_na_size
    from source

)

select * from renamed
