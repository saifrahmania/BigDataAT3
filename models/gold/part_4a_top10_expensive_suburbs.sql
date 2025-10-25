{{ config(materialized='table', schema='dbt_mrahman_gold') }}

SELECT
    lga_name,
    ROUND(AVG(price), 2) AS avg_price
FROM {{ ref('part_3_gold_transform') }}
GROUP BY lga_name
ORDER BY avg_price DESC
LIMIT 10




