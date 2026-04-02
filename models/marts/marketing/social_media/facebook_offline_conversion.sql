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
    )

select
    email,
    phone,
    fn,
    ln,
    country,
    zip,
    dob,
    gen,
    order_id,
    event_name,
    currency,
    event_time,
    value
from facebook_offline_conversion
where email is not null