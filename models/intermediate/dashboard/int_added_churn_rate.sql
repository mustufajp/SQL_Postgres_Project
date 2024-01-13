
with sales as (
    select *
    from {{ ref('int_added_first_purchase_date') }}   
),

churn_rate as (
    select
    year_month,
    churn as montly_churn_rate
    from {{ ref('int_churn_analysis') }}
),

int_added_churn_rate as (

    select *
    from sales
    left join churn_rate
    on date_trunc('month',sales.sales_date)=churn_rate.year_month
)

select *
from int_added_churn_rate
