with
customer_list as (
    select *
    from {{ ref('int_sales_aggregated_to_customer') }}
    order by customer_created_at desc
),
    renamed as (
        select  
         customer_email as email,
        concat('81', substring(cast(customer_phone_number as varchar), 2)) as phone,
        customer_first_name as fn,
        customer_last_name as ln,
        'jp' as country,
        customer_post_code as zip,
        customer_date_of_birth as dob,
        case when customer_gender= 'male' then 'M' when customer_gender='female' then 'F' else null end as gen,
        customer_created_at,
        COALESCE(sales_amount, 0) as sales_amount,
        customer_id
        from customer_list
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
            sales_amount,
            customer_id
    from renamed
    --where customer_created_at >= '2026-09-01' 

