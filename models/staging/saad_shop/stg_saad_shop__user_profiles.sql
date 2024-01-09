with 

source as (

    select * from {{ source('saad_shop', 'user_profiles') }}

),

renamed as (

    select
        user_id
        ,first_name
        ,last_name

    from source

)

select * from renamed
