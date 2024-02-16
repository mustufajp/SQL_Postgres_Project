with 
customer_analysis_dashboard as 
(
    select 
   *,
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

    from {{ ref('int_churn_analysis_added_to_sales') }}
)

select 
*
from customer_analysis_dashboard
