with 

source as (

    select * from {{ source('saad_shop', 'stores') }}

),

renamed as (

    select
        id as store_id,
        short as short_store_name,
        name as store_name,
        address_id as store_address_id,
        phone_number as store_phone_nb,
        created_at as store_inception_at,
        updated_at as store_updated_at,
        hidden as is_store_hidden,
        ecom_inv_hidden as is_ecom_inv_hidden

    from source
)

select * from renamed
