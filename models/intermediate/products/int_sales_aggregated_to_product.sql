WITH 
sku_transactions as 
(
    SELECT *
    FROM {{ ref('stg_saad_shop__sku_transactions') }}
)
,skus as 
(
    SELECT *
    FROM {{ ref('stg_saad_shop__skus') }}
)

SELECT 
*
FROM
 sku_transactions
LEFT JOIN
 skus
using (sku_id)
ORDER BY transaction_id
