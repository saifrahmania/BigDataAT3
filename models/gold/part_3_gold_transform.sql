WITH base AS (
    SELECT * FROM {{ ref('part_2_silver_transform') }}
),

dim_host AS (
    SELECT DISTINCT
        host_id,
        host_name,
        host_is_superhost,
        COUNT(listing_id) AS total_listings
    FROM base
    GROUP BY 1, 2, 3
),

dim_property AS (
    SELECT DISTINCT
        property_type,
        room_type,
        AVG(price) AS avg_price,
        AVG(accommodates) AS avg_capacity
    FROM base
    GROUP BY 1, 2
),

fact_listing AS (
    SELECT
        listing_id,
        host_id,
        lga_name,
        price,
        minimum_nights,
        number_of_reviews,
        median_age,
        median_rent_weekly,
        median_tot_prsnl_inc_weekly
    FROM base
)

SELECT
    f.listing_id,
    f.lga_name,
    f.price,
    f.number_of_reviews,
    f.median_rent_weekly,
    f.median_tot_prsnl_inc_weekly,
    p.property_type,
    p.room_type,
    p.avg_price,
    h.host_name,
    h.host_is_superhost
FROM fact_listing f
LEFT JOIN dim_property p USING (property_type, room_type)
LEFT JOIN dim_host h USING (host_id);