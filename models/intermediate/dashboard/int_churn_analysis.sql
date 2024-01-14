with

    sales as (select * from {{ ref("int_joined_sales_emolyee_customer_store_info") }}),

    monthly_churn as (
        select
            date_trunc('month', sales_date) as year_month,
            customer_age,
            customer_category,
            is_customer_email_verified,
            is_magazine_subscribed,
            is_agree_to_term_of_service,
            customer_prefecture,
            employee_type,
            store_short_name,
            store_prefecture,
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
            ) as last_purchase_customers

        from sales
        group by year_month,
            customer_age,
            customer_category,
            is_customer_email_verified,
            is_magazine_subscribed,
            is_agree_to_term_of_service,
            customer_prefecture,
            employee_type,
            store_short_name,
            store_prefecture
    ),

    churn_rate as (
        select
            t1.year_month,
            cast( t1.distinct_customers as float) as distinct_customers,
            
            t1.customer_age,
            t1.customer_category,
            t1.is_customer_email_verified,
            t1.is_magazine_subscribed,
            t1.is_agree_to_term_of_service,
            t1.customer_prefecture,
            t1.employee_type,
            t1.store_short_name,
            t1.store_prefecture,
            cast(t2.last_purchase_customers as float) as churned_customers
        from monthly_churn as t1
        left join
            monthly_churn as t2 on t1.year_month = t2.year_month + interval '1 month'
    ),

    int_churn_analysis as (
        select
            year_month,
           sum(churned_customers)
            / nullif(sum(distinct_customers), 0) as churn
        from churn_rate
        group by year_month )

select *
from int_churn_analysis
