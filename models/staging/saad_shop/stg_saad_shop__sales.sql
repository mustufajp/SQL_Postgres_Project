with 

source as (

    select * from {{ source('saad_shop', 'sales') }}

),

renamed as (

    select
        id AS sales_id,
        transaction_id,
        amount AS sales_amount,
        type AS sales_type,
        employee_id,
        store_id,
        points_given,
        points_used,
        total_discount,
        estimated_age AS customer_age,
        gender AS customer_category,
        COALESCE(REPLACE((methods->0->>'type')::text, '"', ''), '') AS customer_payment_type,
        COALESCE(REPLACE((methods->0->>'method')::text, '"', ''), '') AS customer_payment_method,
        created_at AS sales_at,
        CAST(created_at AS DATE) as sales_date,
        affiliate_commission_point,
        affiliate_commission_amount,
        (custom_discount->>'price')::numeric AS custom_discount_price,
        (custom_discount->>'coupon')::numeric AS custom_discount_coupon,
        custom_point_back

    from source
)

select * from renamed
