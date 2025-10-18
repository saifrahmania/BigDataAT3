-- Mart 2: aggregated statistics per LGA

WITH base AS (
    SELECT * FROM {{ ref('listings_base') }}
),

agg AS (
    SELECT
        LOWER(TRIM(listing_neighbourhood)) AS lga,
        COUNT(listing_id) AS total_listings,
        ROUND(AVG(price), 2) AS avg_price,
        ROUND(AVG(review_scores_rating), 2) AS avg_rating,
        ROUND(AVG(accommodates), 2) AS avg_capacity
    FROM base
    GROUP BY LOWER(TRIM(listing_neighbourhood))
)

SELECT * FROM agg
