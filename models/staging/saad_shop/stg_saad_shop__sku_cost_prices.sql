with 

source as (

    select * from {{ source('saad_shop', 'sku_cost_prices') }}

),

renamed as (

    select
    sku_code,
    sku_id,
    cost_unit_amount,
    created_at
    from source

)

select * from renamed
