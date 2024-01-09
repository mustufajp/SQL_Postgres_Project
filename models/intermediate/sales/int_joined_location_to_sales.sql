with
    int_joined_sales_transaction_employee_store_info as (
        select * from {{ ref("int_joined_sales_transaction_employee_store_info") }}
    ),

    stg_saad_shop__addresse as (select * from {{ ref("stg_saad_shop__addresses") }}),

    int_joined_location_to_sales as (
        select
            s.*,
            store.combined_address as store_full_address,
            store.prefecture as store_prefecture,
            store.city as store_city,
            customer.combined_address as customer_full_address,
            customer.prefecture as customer_prefecture,
            customer.city as customer_city
        from int_joined_sales_transaction_employee_store_info as s
        left join
            stg_saad_shop__addresse as store on store.address_id = s.store_address_id
        left join
            stg_saad_shop__addresse as customer on customer.user_id = s.customer_id
        order by transaction_id
    )

select *
from int_joined_location_to_sales
