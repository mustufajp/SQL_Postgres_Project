with
stores as (select * from {{ ref("stg_saad_shop__stores") }}),
addresses as (select * from {{ ref("stg_saad_shop__addresses") }}),

int_address_joined_to_store as (
    select*
    from stores
    left join addresses
    on store_address_id = address_id
)

select *

from int_address_joined_to_store