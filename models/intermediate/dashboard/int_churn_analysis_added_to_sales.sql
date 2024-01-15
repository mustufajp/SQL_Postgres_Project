with 
sales as (

    select *
    from {{ ref('int_joined_sales_emolyee_customer_store_info') }}
),

churn_rate as (
    select *
    from {{ ref('int_churn_analysis') }}
),

int_churn_analysis_added_to_sales as (

    select *
    from sales
    left join churn_rate
    on date_trunc('month',sales.sales_date)=churn_rate.year_month
)

select *
from int_churn_analysis_added_to_sales