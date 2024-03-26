with 

source as (

    select * from {{ source('saad_shop', 'sku_cost_prices') }}

),

renamed as (

    select
    *
    from source

)

select * from renamed
