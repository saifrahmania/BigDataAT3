-- models/core/part_2.sql
-- Stage 2: Core transformation - cleaned listings base

WITH listings_raw AS (
    SELECT *
    FROM {{ ref('listings_raw') }}
),

typed AS (
    SELECT
        CAST(listing_id AS BIGINT)             AS listing_id,
        CAST(host_id AS BIGINT)                AS host_id,
        NULLIF(host_name, '')                  AS host_name,
        CASE LOWER(host_is_superhost)
            WHEN 't' THEN TRUE WHEN 'true' THEN TRUE WHEN '1' THEN TRUE
            ELSE FALSE END AS host_is_superhost,
        NULLIF(host_neighbourhood, '')         AS host_neighbourhood,
        TO_DATE(host_since, 'YYYY-MM-DD')      AS host_since,
        NULLIF(listing_neighbourhood, '')      AS listing_neighbourhood,
        NULLIF(property_type, '')              AS property_type,
        NULLIF(room_type, '')                  AS room_type,
        NULLIF(accommodates, '')::INT          AS accommodates,
        NULLIF(availability_30, '')::INT       AS availability_30,
        CASE LOWER(has_availability)
            WHEN 't' THEN TRUE WHEN 'true' THEN TRUE WHEN '1' THEN TRUE
            ELSE FALSE END AS has_availability,
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
    FROM listings_raw
)

SELECT * FROM typed
