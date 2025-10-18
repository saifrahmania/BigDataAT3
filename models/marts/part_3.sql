-- ==========================================
-- PART 3: Monthly Aggregation Model (Bronze → Silver)
-- dbt-safe version: no INSERT/DELETE; pure SELECT
-- ==========================================

WITH typed AS (
    SELECT
        CAST(listing_id AS BIGINT)             AS listing_id,
        CAST(host_id AS BIGINT)                AS host_id,
        NULLIF(host_name, '')                  AS host_name,
        CASE LOWER(host_is_superhost)
            WHEN 't' THEN TRUE WHEN 'true' THEN TRUE WHEN '1' THEN TRUE
            ELSE FALSE END                     AS host_is_superhost,
        NULLIF(host_neighbourhood, '')         AS host_neighbourhood,
        TO_DATE(host_since, 'YYYY-MM-DD')      AS host_since,
        NULLIF(listing_neighbourhood, '')      AS listing_neighbourhood,
        NULLIF(property_type, '')              AS property_type,
        NULLIF(room_type, '')                  AS room_type,
        NULLIF(accommodates, '')::INT          AS accommodates,
        NULLIF(availability_30, '')::INT       AS availability_30,
        CASE LOWER(has_availability)
            WHEN 't' THEN TRUE WHEN 'true' THEN TRUE WHEN '1' THEN TRUE
            ELSE FALSE END                     AS has_availability,
        NULLIF(number_of_reviews, '')::INT     AS number_of_reviews,
        REPLACE(REPLACE(NULLIF(price, ''), '$', ''), ',', '')::NUMERIC(12,2) AS price,
        NULLIF(review_scores_rating, '')::NUMERIC(6,2)          AS review_scores_rating,
        NULLIF(review_scores_accuracy, '')::NUMERIC(6,2)        AS review_scores_accuracy,
        NULLIF(review_scores_cleanliness, '')::NUMERIC(6,2)     AS review_scores_cleanliness,
        NULLIF(review_scores_checkin, '')::NUMERIC(6,2)         AS review_scores_checkin,
        NULLIF(review_scores_communication, '')::NUMERIC(6,2)   AS review_scores_communication,
        NULLIF(review_scores_value, '')::NUMERIC(6,2)           AS review_scores_value,
        NULLIF(scrape_id, '')::BIGINT           AS scrape_id,
        TO_DATE(scraped_date, 'YYYY-MM-DD')     AS scraped_date,
        year_month
    FROM {{ ref('listings_raw') }}
)

, monthly_rollup AS (
    SELECT
        listing_id,
        year_month,
        MAX(has_availability) AS has_availability,
        MAX(CASE WHEN has_availability THEN price END) AS price,
        ROUND(AVG(availability_30))::INT AS availability_30,
        SUM(number_of_reviews) AS number_of_reviews,
        AVG(review_scores_rating) AS review_scores_rating,
        (ARRAY_AGG(room_type ORDER BY scraped_date DESC))[1] AS room_type,
        (ARRAY_AGG(property_type ORDER BY scraped_date DESC))[1] AS property_type,
        (ARRAY_AGG(accommodates ORDER BY scraped_date DESC))[1] AS accommodates,
        (ARRAY_AGG(listing_neighbourhood ORDER BY scraped_date DESC))[1] AS listing_neighbourhood,
        (ARRAY_AGG(host_id ORDER BY scraped_date DESC))[1] AS host_id,
        GREATEST(0, 30 - ROUND(AVG(availability_30))::INT) AS stays,
        COALESCE(MAX(CASE WHEN has_availability THEN price END),0)
        * GREATEST(0, 30 - ROUND(AVG(availability_30))::INT) AS estimated_revenue
    FROM typed
    GROUP BY listing_id, year_month
)

SELECT * FROM monthly_rollup
