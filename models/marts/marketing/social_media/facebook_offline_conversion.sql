With customer_info as 
(
    select *
    from {{ ref('facebook_custom_audience') }}
),

sales_info as 
(
    select 
    transaction_id,
    customer_id,
    'Purchase' as event_name,
    'JPY' as currency,
    sales_at as event_time,
    sales_amount as value

    from {{ ref('customer_analysis_dashboard') }}
),

facebook_offline_conversion as 
(
    select *
    from sales_info
    left join customer_info using (customer_id)
)

select *
from facebook_offline_conversion
-- where date_trunc('day',event_time) between '2025-06-03' and '2025-06-15'  and customer_id is not null

