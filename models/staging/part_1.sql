-- Stage 1: Bronze listings_raw data
-- This model extracts the raw seed data and standardizes column naming.

WITH listings_raw AS (
    SELECT
        listing_id AS listing_id,
        scrape_id AS scrape_id,
        scraped_date AS scraped_date,
        host_id AS host_id,
        host_name AS host_name,
        host_since AS host_since,
        host_is_superhost AS host_is_superhost,
        host_neighbourhood AS host_neighbourhood,
        listing_neighbourhood AS listing_neighbourhood,
        property_type AS property_type,
        room_type AS room_type,
        accommodates AS accommodates,
        price AS price,
        has_availability AS has_availability,
        availability_30 AS availability_30,
        number_of_reviews AS number_of_reviews,
        review_scores_rating AS review_scores_rating,
        review_scores_accuracy AS review_scores_accuracy,
        review_scores_cleanliness AS review_scores_cleanliness,
        review_scores_checkin AS review_scores_checkin,
        review_scores_communication AS review_scores_communication,
        review_scores_value AS review_scores_value,
        source_file AS source_file,
        year_month AS year_month

    FROM {{ ref('listings_raw_union') }}
)

SELECT * FROM listings_raw