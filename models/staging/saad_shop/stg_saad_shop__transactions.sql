with 

source as (

    select * from {{ source('saad_shop', 'transactions') }}

),

renamed as (

    select
        id as transaction_id
        ,amount as transaction_amount
        ,point_amount as transaction_point_amount
        ,real_world_amount as transaction_real_world_amount
        ,sender_id as customer_id
        ,created_at as transaction_date
        ,type as transaction_type
        ,parent_transaction_id

    from source

)

select * from renamed
