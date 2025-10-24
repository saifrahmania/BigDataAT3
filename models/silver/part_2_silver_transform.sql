WITH airbnb_clean AS (
    SELECT
        listing_id,
        host_id,
        host_name,
        host_is_superhost,
        property_type,
        room_type,
        accommodates,
        price::NUMERIC AS price,
        minimum_nights::INT,
        number_of_reviews::INT,
        neighbourhood_cleansed AS suburb,
        '2020-05-01'::DATE AS scraped_date
    FROM bronze.raw_listings
    WHERE price IS NOT NULL
),
lga_map AS (
    SELECT
        suburb,
        lga_name,
        lga_code
    FROM bronze.nsw_lga_suburb
),
census_g01 AS (
    SELECT
        lga_code,
        median_age::INT,
        median_mortgage_repay_monthly::NUMERIC,
        median_rent_weekly::NUMERIC
    FROM bronze."2016census_g01_nsw_lga"
),
census_g02 AS (
    SELECT
        lga_code,
        avg_household_size::NUMERIC,
        median_tot_prsnl_inc_weekly::NUMERIC
    FROM bronze."2016census_g02_nsw_lga"
)

SELECT
    a.listing_id,
    a.host_id,
    a.host_name,
    a.host_is_superhost,
    a.property_type,
    a.room_type,
    a.accommodates,
    a.price,
    a.minimum_nights,
    a.number_of_reviews,
    l.lga_name,
    l.lga_code,
    c1.median_age,
    c1.median_mortgage_repay_monthly,
    c1.median_rent_weekly,
    c2.avg_household_size,
    c2.median_tot_prsnl_inc_weekly
FROM airbnb_clean a
LEFT JOIN lga_map l ON a.suburb = l.suburb
LEFT JOIN census_g01 c1 ON l.lga_code = c1.lga_code
LEFT JOIN census_g02 c2 ON l.lga_code = c2.lga_code;