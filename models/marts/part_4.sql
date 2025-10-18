-- ==========================================
-- PART 4: Ad-hoc analysis queries (dbt version)
-- ==========================================

WITH lga_map AS (
    SELECT
        LOWER(TRIM(suburb_name)) AS suburb,
        TRIM(lga_name)           AS lga_name
    FROM {{ ref('nsw_lga_suburb_silver') }}
),
base AS (
    SELECT
        f.*,
        COALESCE(l.lga_name, f.listing_neighbourhood) AS lga_name
    FROM {{ ref('fact_listing_monthly') }} f
    LEFT JOIN lga_map l
        ON LOWER(TRIM(f.listing_neighbourhood)) = l.suburb
),
last_12 AS (
    SELECT *
    FROM base
    WHERE year_month BETWEEN '2020-05' AND '2021-04'
),
lga_perf AS (
    SELECT
        lga_name,
        SUM(CASE WHEN has_availability THEN estimated_revenue ELSE 0 END) AS revenue_active_sum,
        NULLIF(SUM(CASE WHEN has_availability THEN 1 ELSE 0 END),0)      AS active_listing_months
    FROM last_12
    GROUP BY lga_name
),
lga_perf_rate AS (
    SELECT
        lga_name,
        (revenue_active_sum / active_listing_months)::NUMERIC(14,2) AS revenue_per_active_listing
    FROM lga_perf
    WHERE active_listing_months IS NOT NULL
),
census_g01 AS (
    SELECT
        lga_name,
        median_age_persons::NUMERIC AS median_age,
        average_household_size::NUMERIC AS avg_household_size
    FROM {{ ref('census_g01_silver') }}
),
census_g02 AS (
    SELECT
        lga_name,
        median_mortgage_repay_monthly::NUMERIC AS median_mortgage_repay_monthly
    FROM {{ ref('census_g02_silver') }}
),
lga_enriched AS (
    SELECT
        p.lga_name,
        p.revenue_per_active_listing,
        g01.median_age,
        g01.avg_household_size,
        g02.median_mortgage_repay_monthly
    FROM lga_perf_rate p
    LEFT JOIN census_g01 g01 USING (lga_name)
    LEFT JOIN census_g02 g02 USING (lga_name)
),
ranked AS (
    SELECT
        lga_name,
        revenue_per_active_listing,
        median_age,
        avg_household_size,
        RANK() OVER (ORDER BY revenue_per_active_listing DESC) AS rk_desc,
        RANK() OVER (ORDER BY revenue_per_active_listing ASC)  AS rk_asc
    FROM lga_enriched
),
top13 AS (
    SELECT * FROM ranked WHERE rk_desc <= 13
),
bottom13 AS (
    SELECT * FROM ranked WHERE rk_asc <= 13
)
SELECT
    'TOP_13'  AS bucket,
    AVG(median_age)         AS avg_median_age,
    AVG(avg_household_size) AS avg_household_size,
    AVG(revenue_per_active_listing) AS avg_rev_per_active
FROM top13
UNION ALL
SELECT
    'BOTTOM_13',
    AVG(median_age),
    AVG(avg_household_size),
    AVG(revenue_per_active_listing)
FROM bottom13;
