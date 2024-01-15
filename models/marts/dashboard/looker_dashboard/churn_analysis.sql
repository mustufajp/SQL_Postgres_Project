with 

churn_rate as (
    select *
    from {{ ref('int_churn_analysis_segment') }}
)

select *
from churn_rate

