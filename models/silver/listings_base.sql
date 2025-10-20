-- Silver layer: further normalization and enrichment

WITH cleaned AS (
    SELECT * 
    FROM {{ ref('listings_raw_union') }}   -- ✅ this pulls from your bronze union model
),

standardized AS (
    SELECT
        listing_id,
        host_id,
        LOWER(TRIM(host_name)) AS host_name,
        host_is_superhost AS is_superhost,
        LOWER(TRIM(host_neighbourhood)) AS host_neighbourhood,
        LOWER(TRIM(listing_neighbourhood)) AS suburb_name,
        LOWER(TRIM(property_type)) AS property_type,
        LOWER(TRIM(room_type)) AS room_type,
        accommodates,
        price,
        availability_30,
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value,
        year_month
    FROM cleaned
    WHERE listing_id IS NOT NULL
)

SELECT * FROM standardized