with
    customer_info as (
        select customer_id, 
        email, 
        phone, 
        fn, 
        ln, 
        country, 
        zip, 
        dob, 
        gen
        from {{ ref("facebook_custom_audience") }}
    ),

    sales_info as (
        select
            customer_id,
            transaction_id as order_id,
            'Purchase' as event_name,
            'JPY' as currency,
            sales_at as event_time,
            sales_amount as value

        from {{ ref("customer_analysis_dashboard") }}
    ),

    facebook_offline_conversion as (
        select * from sales_info left join customer_info using (customer_id)
    ),
select_data as (
select
    email as email_address,
    phone,
    fn as first_name,
    ln as last_name,
    country,
    zip,
    dob,
    gen as gender,
    order_id,
    event_time as date_created,
    event_name as conversion_name,
    currency,
    value as conversion_value,
    'GRANTED' AS ad_user_data_consent,
    'GRANTED' AS ad_personalization_consent
from facebook_offline_conversion
where email is not null 
)

select
*
from select_data
