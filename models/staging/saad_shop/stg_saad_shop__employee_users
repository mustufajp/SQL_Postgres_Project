with 

source as (

    select * from {{ source('saad_shop', 'employee_users') }}

),

renamed as (

    select
        user_id as employee_id
        ,store_id as employee_store_id
        ,type as employee_type
        ,active as employee_status

    from source

)

select * from renamed
