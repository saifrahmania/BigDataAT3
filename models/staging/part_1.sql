-- Stage 1: Bronze listings_raw data
-- This model extracts the raw seed data and standardizes column naming.

WITH listings_raw AS (
    SELECT
        "LISTING_ID" AS listing_id,
        "SCRAPE_ID" AS scrape_id,
        "SCRAPED_DATE" AS scraped_date,
        "HOST_ID" AS host_id,
        "HOST_NAME" AS host_name,
        "HOST_SINCE" AS host_since,
        "HOST_IS_SUPERHOST" AS host_is_superhost,
        "HOST_NEIGHBOURHOOD" AS host_neighbourhood,
        "LISTING_NEIGHBOURHOOD" AS listing_neighbourhood,
        "PROPERTY_TYPE" AS property_type,
        "ROOM_TYPE" AS room_type,
        "ACCOMMODATES" AS accommodates,
        "PRICE" AS price,
        "HAS_AVAILABILITY" AS has_availability,
        "AVAILABILITY_30" AS availability_30,
        "NUMBER_OF_REVIEWS" AS number_of_reviews,
        "REVIEW_SCORES_RATING" AS review_scores_rating,
        "REVIEW_SCORES_ACCURACY" AS review_scores_accuracy,
        "REVIEW_SCORES_CLEANLINESS" AS review_scores_cleanliness,
        "REVIEW_SCORES_CHECKIN" AS review_scores_checkin,
        "REVIEW_SCORES_COMMUNICATION" AS review_scores_communication,
        "REVIEW_SCORES_VALUE" AS review_scores_value,
        "SOURCE_FILE" AS source_file,
        "YEAR_MONTH" AS year_month
    FROM {{ ref('listings_raw') }}
)

SELECT * FROM listings_raw
