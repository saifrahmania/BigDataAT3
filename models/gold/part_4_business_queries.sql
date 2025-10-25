-- 1️⃣ Top 10 expensive suburbs
SELECT
    lga_name,
    ROUND(AVG(price), 2) AS avg_price
FROM {{ ref('part_3_gold_transform') }}
GROUP BY lga_name
ORDER BY avg_price DESC
LIMIT 10;

-- 2️⃣ Superhosts vs Non-superhosts average prices
SELECT
    host_is_superhost,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(listing_id) AS listings
FROM {{ ref('part_3_gold_transform') }}
GROUP BY host_is_superhost;

-- 3️⃣ Room type distribution per LGA
SELECT
    lga_name,
    room_type,
    COUNT(listing_id) AS num_listings
FROM {{ ref('part_3_gold_transform') }}
GROUP BY lga_name, room_type