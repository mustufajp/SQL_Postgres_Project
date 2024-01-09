with 
int_joined_user_info_to_sales as 
(
    select *
    from {{ ref('int_joined_user_info_to_sales') }}
),

sales as (select 
{{ dbt_utils.star(from=ref('int_joined_user_info_to_sales'), except=[
    "affiliate_commission_point",
    "affiliate_commission_amount",
    "custom_discount_price",
    "custom_discount_coupon",
    "parent_transaction_id",
    "store_address_id",
    "employee_id",
    "user_id"
]) }}
from int_joined_user_info_to_sales)

select 
 *
from sales
