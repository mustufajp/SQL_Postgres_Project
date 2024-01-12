with

sales as (

    select *
    from {{ ref('int_added_first_purchase_date') }}
),

monthly_churn as (
    select
    date_trunc('month', sales_date) as year_month,
    count (distinct case when date_trunc('month', sales_date)=date_trunc('month', last_purchase) then customer_id end) as last_purchase_customers,
    count(distinct customer_id) as distinct_customers
    from sales
    group by date_trunc('month', sales_date)
),

churn_rate AS (
    SELECT
        t1.year_month,
        t1.distinct_customers,
        t2.last_purchase_customers AS churned_customers
    FROM
        monthly_churn AS t1
    LEFT JOIN
        monthly_churn AS t2 ON t1.year_month = t2.year_month + interval '1 month'
        ),

int_churn_analysis as (
    select 
        year_month,
        (CAST ( churned_customer AS float )/CAST ( distinct_customers AS float ))*100 as churn_rate
        from churn_rate
),

select *
from int_churn_analysis