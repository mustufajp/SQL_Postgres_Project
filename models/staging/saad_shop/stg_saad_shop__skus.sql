with 

source as (

    select * from {{ source('saad_shop', 'skus') }}

),

renamed as (

    select
        id as sku_id
        ,sku_code
        ,price as current_price
        ,created_at as product_inception_date
        ,updated_at as product_updated_date
        ,category as product_category
        ,code as product_code
        ,type as product_type
        ,size as product_size
    from source

)

select * from renamed
