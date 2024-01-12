with
hubspot_contacts as (
    select 
        customer_id,
        customer_phone_number,
        customer_email,
        customer_first_name,
        customer_last_name,
        customer_gender,
        customer_date_of_birth,
        customer_post_code,
        customer_prefecture,
        customer_city,
        customer_town,
        customer_country,
        customer_combined_address
    from {{ ref('int_add_addresses_to_user_info') }}
    )

select * from hubspot_contacts