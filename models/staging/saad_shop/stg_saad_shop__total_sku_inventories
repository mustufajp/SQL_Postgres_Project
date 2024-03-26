with 

source as (

    select * from {{ source('saad_shop', 'total_sku_inventories') }}

),

renamed as (

    select
        sku_id,
        sum as total_product_available

    from source

)

select * from renamed
