-- models/staging/part_1.sql
-- Stage 1: Bronze listings_raw data

WITH listings_raw AS (
    SELECT
        listing_id,
        scrape_id,
        scraped_date,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value,
        source_file,
        year_month,
        _src_file,
        _ingested_at
    FROM {{ ref('listings_raw') }}
)

SELECT * FROM listings_raw;
