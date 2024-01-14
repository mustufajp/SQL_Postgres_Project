with 

churn_rate as (
    select *
    from {{ ref('int_churn_analysis') }}
)

select *
from churn_rate