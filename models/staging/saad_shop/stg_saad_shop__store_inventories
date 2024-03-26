with

    source as (select * from {{ source("saad_shop", "store_inventories") }}),

    renamed as (
        select 
        store_id, 
        sku_id, 
        inventory 
        from source)

select *
from renamed
