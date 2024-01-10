with
stores as (
    select 
        store_id,
        store_short_name,
        store_name,
        store_address_id,
        store_phone_number,
        store_created_at,
        store_updated_at,
        is_store_hidden,
        is_store_ecom_inv_hidden
    from {{ ref("stg_saad_shop__stores") }}
    ),
addresses as (
    select 
        address_id,
        post_code as store_post_code,
        prefecture as store_prefecture,
        city as store_city,
        town as store_town,
        line_one as store_line_one,
        line_two as store_line_two,
        latitude as store_latitude,
        longitude as store_longtitude,
        country as store_country,
        combined_address as store_combined_address
     from {{ ref("stg_saad_shop__addresses") }}
     ),

int_address_joined_to_store as (
    select *
    from stores
    left join addresses
    on store_address_id = address_id
)

select *
from int_address_joined_to_store

