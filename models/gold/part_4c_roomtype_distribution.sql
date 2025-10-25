{{ config(materialized='table', schema='dbt_mrahman_gold') }}
SELECT
    lga_name,
    room_type,
    COUNT(listing_id) AS num_listings
FROM {{ ref('part_3_gold_transform') }}
GROUP BY lga_name, room_type