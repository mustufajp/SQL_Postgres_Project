with 

source as (

    select * from {{ source('saad_shop', 'stores') }}

),

renamed as (

    select
        id as store_id,
        short as store_short_name,
        name as store_name,
        address_id as store_address_id,
        phone_number as store_phone_number,
        store_image,
        created_at as store_created_at,
        updated_at as store_updated_at,
        hidden as is_store_hidden,
        ecom_inv_hidden as is_store_ecom_inv_hidden,
        closed as is_store_closed

    from source

)

select * from renamed
