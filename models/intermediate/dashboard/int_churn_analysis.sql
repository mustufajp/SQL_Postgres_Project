with

    sales as (
        select * from {{ ref("int_joined_sales_emolyee_customer_store_info") }}
        ),

    churn_calc as (
        select
            date_trunc('month', sales_date) as year_month,
            count(distinct customer_id) as distinct_customers,
            count(
                distinct case
                    when
                        date_trunc('month', sales_date) + interval '1 month'
                        != date_trunc('month', current_date)
                        and date_trunc('month', sales_date)
                        = date_trunc('month', last_purchase)
                    then customer_id

                    when
                        date_trunc('month', sales_date) + interval '1 month'
                        = date_trunc('month', current_date)
                        and sales_date
                        between date_trunc('month', sales_date) and current_date
                        - interval '1 month'
                        and date_trunc('month', sales_date)
                        = date_trunc('month', last_purchase)
                    then customer_id
                end
            ) as churned_customers

        from sales
        group by 
            year_month
    ),

    churn_calc_join as (
        select
            t1.year_month,
            t1.distinct_customers,
            t2.churned_customers
        from churn_calc as t1
        left join
            churn_calc as t2 on t1.year_month = t2.year_month + interval '1 month'
    ),

     
   int_churn_analysis as (
        select
            year_month,
           cast(sum(churned_customers)
            / nullif(sum(distinct_customers), 0) as float) as churn
        from churn_calc_join
        group by year_month )

select *
from int_churn_analysis
