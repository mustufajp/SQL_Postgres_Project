with
int_joined_location_to_sales as 
(
    select *
    from {{ ref('int_joined_location_to_sales') }}
),
stg_saad_shop__user_profiles as 
(
select *
from {{ ref('stg_saad_shop__user_profiles') }}
)


SELECT 
c.*
,p.first_name as employee_first_name
,p.last_name as employee_last_name
FROM int_joined_location_to_sales as c
LEFT JOIN  stg_saad_shop__user_profiles as p
ON p.user_id = c.employee_id

