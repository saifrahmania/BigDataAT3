-- Core transformation: cleaning and type conversions from staging layer.

WITH base AS (
    SELECT * FROM {{ ref('part_1') }}
),

typed AS (
    SELECT
        CAST(listing_id AS BIGINT) AS listing_id,
        CAST(scrape_id AS BIGINT) AS scrape_id,
        CAST(host_id AS BIGINT) AS host_id,
        host_name,
        TO_DATE(host_since, 'DD/MM/YYYY') AS host_since,
        CASE 
            WHEN host_is_superhost IN ('t', 'true', '1') THEN TRUE
            ELSE FALSE
        END AS is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates::INT AS accommodates,
        price::NUMERIC AS price,
        has_availability,
        availability_30::INT AS availability_30,
        number_of_reviews::INT AS number_of_reviews,
        review_scores_rating::NUMERIC AS review_scores_rating,
        review_scores_accuracy::NUMERIC AS review_scores_accuracy,
        review_scores_cleanliness::NUMERIC AS review_scores_cleanliness,
        review_scores_checkin::NUMERIC AS review_scores_checkin,
        review_scores_communication::NUMERIC AS review_scores_communication,
        review_scores_value::NUMERIC AS review_scores_value,
        source_file,
        year_month::DATE AS year_month
    FROM base
)

SELECT * FROM typed
