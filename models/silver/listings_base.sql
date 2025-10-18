-- models/silver/listings_base.sql

WITH typed AS (
  SELECT
    CAST("LISTING_ID" AS BIGINT)             AS listing_id,
    CAST("HOST_ID" AS BIGINT)                AS host_id,
    NULLIF("HOST_NAME",'')                   AS host_name,
    CASE LOWER("HOST_IS_SUPERHOST") WHEN 't' THEN TRUE WHEN 'true' THEN TRUE WHEN '1' THEN TRUE ELSE FALSE END AS host_is_superhost,
    NULLIF("HOST_NEIGHBOURHOOD",'')          AS host_neighbourhood,
    TO_DATE("HOST_SINCE",'YYYY-MM-DD')       AS host_since,
    NULLIF("LISTING_NEIGHBOURHOOD",'')       AS listing_neighbourhood,
    NULLIF("PROPERTY_TYPE",'')               AS property_type,
    NULLIF("ROOM_TYPE",'')                   AS room_type,
    NULLIF("ACCOMMODATES",'')::INT           AS accommodates,
    NULLIF("AVAILABILITY_30",'')::INT        AS availability_30,
    CASE LOWER("HAS_AVAILABILITY") WHEN 't' THEN TRUE WHEN 'true' THEN TRUE WHEN '1' THEN TRUE ELSE FALSE END AS has_availability,
    NULLIF("NUMBER_OF_REVIEWS",'')::INT      AS number_of_reviews,
    REPLACE(REPLACE(NULLIF("PRICE",''),'$',''),',','')::NUMERIC(12,2) AS price,
    NULLIF("REVIEW_SCORES_RATING",'')::NUMERIC(6,2)          AS review_scores_rating,
    NULLIF("REVIEW_SCORES_ACCURACY",'')::NUMERIC(6,2)        AS review_scores_accuracy,
    NULLIF("REVIEW_SCORES_CLEANLINESS",'')::NUMERIC(6,2)     AS review_scores_cleanliness,
    NULLIF("REVIEW_SCORES_CHECKIN",'')::NUMERIC(6,2)         AS review_scores_checkin,
    NULLIF("REVIEW_SCORES_COMMUNICATION",'')::NUMERIC(6,2)   AS review_scores_communication,
    NULLIF("REVIEW_SCORES_VALUE",'')::NUMERIC(6,2)           AS review_scores_value,
    NULLIF("SCRAPE_ID",'')::BIGINT           AS scrape_id,
    TO_DATE("SCRAPED_DATE",'YYYY-MM-DD')     AS scraped_date,
    "YEAR_MONTH"                              AS year_month
  FROM {{ ref('listings_raw') }}
)

SELECT * FROM typed
