-- Mart 1: summary of listings per suburb per month

WITH base AS (
    SELECT * FROM {{ ref('listings_base') }}
),

summary AS (
    SELECT
        suburb_name AS suburb,
        year_month,
        COUNT(listing_id) AS total_listings,
        ROUND(AVG(price), 2) AS avg_price,
        ROUND(AVG(review_scores_rating), 2) AS avg_rating
    FROM base
    GROUP BY suburb_name, year_month
)

SELECT * FROM summary
--- 