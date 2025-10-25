{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

WITH host_data AS (
    SELECT
        host_id,
        host_name,
        host_is_superhost,
        COUNT(listing_id) AS total_listings,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(DISTINCT lga_name) AS lga_count
    FROM {{ ref('listings_base') }}
    GROUP BY host_id, host_name, host_is_superhost
)

SELECT * FROM host_data