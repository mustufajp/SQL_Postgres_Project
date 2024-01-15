with 

churn_rate as (
    select 

    {{ dbt_utils.star(from=ref('int_churn_analysis_segment'), except=[
        "distinct_customers",
        "churned_customers", 
        ]) }}
    from {{ ref('int_churn_analysis_segment') }}
)

select *
from churn_rate

