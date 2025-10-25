{{ config(materialized='table', schema='dbt_mrahman_gold') }}
SELECT
    host_is_superhost,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(listing_id) AS listings
FROM {{ ref('part_3_gold_transform') }}
GROUP BY host_is_superhost