with 
sales as 
(
    select 
    *
    from {{ ref('int_added_first_purchase_date') }}
),

int_added_membership_status_to_transaction as (

    select *,
    case 
    when customer_id is not null and sales_date=first_purchase then '新規会員'
    when customer_id is not null then '既存会員'
    when customer_id is null then '非会員'
    end as member_status,
    
    case 
    when customer_id is not null and sales_date=first_purchase then 'New User'
    when customer_id is not null then 'Existing User'
    when customer_id is null then 'Non-User'
    end as member_status_en
    from sales

)

select *
from int_added_membership_status_to_transaction