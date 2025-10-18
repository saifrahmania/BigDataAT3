-- ==========================================
-- PART 3: Monthly Load Placeholders
-- This file shows the SQL you might run inside Airflow tasks or manually.
-- Replace {{YM}} with a value like '2020-05' (or pass via dbt/Airflow variables)
-- ==========================================

-- EXAMPLE 1: Load a month's raw CSV (already merged in your case, so optional)
-- \copy bronze."listings_raw" FROM '/path/to/listings_{{YM}}.csv' CSV HEADER;

-- EXAMPLE 2: (Bronze → Silver) Upsert for the month's slice from bronze to silver
-- Here we assume bronze.listings_raw exists and contains a YEAR_MONTH column.
-- (If you loaded directly to silver via pandas, you can skip this.)

-- Create a temp typed view for the month:
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
  FROM bronze."listings_raw"
  WHERE "YEAR_MONTH" = '{{YM}}'
)
-- Upsert into silver.listings_base for that month
INSERT INTO silver.listings_base
SELECT * FROM typed
ON CONFLICT DO NOTHING;

-- Recompute monthly rollup for that month (idempotent approach: delete+insert)
DELETE FROM silver.listing_monthly WHERE year_month = '{{YM}}';

INSERT INTO silver.listing_monthly (
  listing_id, year_month, has_availability, price, availability_30,
  number_of_reviews, review_scores_rating, room_type, property_type, accommodates,
  listing_neighbourhood, host_id, stays, estimated_revenue
)
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
  COALESCE(MAX(CASE WHEN has_availability THEN price END),0) * GREATEST(0, 30 - ROUND(AVG(availability_30))::INT) AS estimated_revenue
FROM silver.listings_base
WHERE year_month = '{{YM}}'
GROUP BY listing_id, year_month;

-- (Optional) After all months loaded:
-- INSERT/REFRESH gold.fact_listing_monthly from silver.listing_monthly;
-- INSERT/REFRESH gold.dim_host, gold.dim_property from silver.listings_base;