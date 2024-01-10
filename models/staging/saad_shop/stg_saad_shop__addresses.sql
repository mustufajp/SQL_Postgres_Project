with 

source as (

    select * from {{ source('saad_shop', 'addresses') }}

),

renamed as (

    select
        id as address_id,
        post_code,
        prefecture,
        city,
        town,
        line_one,
        line_two,
        full_width_kana,
        latitude,
        longitude,
        user_id,
        case when country is null then 'jp' end as country,
        CONCAT(post_code, ' ', prefecture, ' ', city, ' ', town, ' ', line_one) AS combined_address

    from source
)

select* from renamed
