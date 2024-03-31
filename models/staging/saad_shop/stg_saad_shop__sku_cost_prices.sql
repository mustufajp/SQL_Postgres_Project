with 

source as (

    select * from {{ source('saad_shop', 'sku_cost_prices') }}

),

renamed as (

    select
    sku_code,
    sku_id,
    cost_unit_amount,
    created_at as cost_created_date
    from source

)

select * from renamed
