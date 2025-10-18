-- ==========================================
-- PART 4: Ad-hoc analysis queries
-- NOTE: Adjust column names from Census tables as needed.
-- Assumptions:
--  - gold.fact_listing_monthly has: listing_id, year_month, estimated_revenue, has_availability,
--    price, stays, listing_neighbourhood, host_id, property_type, room_type, accommodates, review_scores_rating
--  - silver.nsw_lga_suburb has suburb->LGA mapping (adjust keys below)
--  - silver.census_g01 and silver.census_g02 are staged as JSONB; replace JSON ->> keys with exact column names.
-- ==========================================

-- ------------------------------------------
-- Helper: map neighbourhood (suburb) to LGA
-- ------------------------------------------
WITH lga_map AS (
  SELECT
    LOWER(TRIM(raw->>'suburb')) AS suburb,
    TRIM(raw->>'lga_name')      AS lga_name
  FROM silver.nsw_lga_suburb
),
base AS (
  SELECT
    f.*,
    COALESCE(l.lga_name, f.listing_neighbourhood) AS lga_name
  FROM gold.fact_listing_monthly f
  LEFT JOIN lga_map l
    ON LOWER(TRIM(f.listing_neighbourhood)) = l.suburb
),
last_12 AS (  -- adjust window as needed
  SELECT * FROM base
  WHERE year_month BETWEEN '2020-05' AND '2021-04'
),

-- 1) LGA revenue per active listing over the window
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

-- Bring in census attributes (adjust keys)
census_g01 AS (
  SELECT
    TRIM(raw->>'lga_name') AS lga_name,
    raw                    AS g01
  FROM silver.census_g01
),
census_g02 AS (
  SELECT
    TRIM(raw->>'lga_name') AS lga_name,
    raw                    AS g02
  FROM silver.census_g02
),
lga_enriched AS (
  SELECT
    p.lga_name,
    p.revenue_per_active_listing,
    -- EXAMPLES (replace keys with actual):
    (g01->>'Median_age_persons')::NUMERIC         AS median_age,     -- if in G01/G02
    (g01->>'Average_household_size')::NUMERIC     AS avg_household_size
  FROM lga_perf_rate p
  LEFT JOIN census_g01 g01 USING (lga_name)
  LEFT JOIN census_g02 g02 USING (lga_name)
)

-- =========================
-- Q1: Demographic differences between
--     Top 13 and Bottom 13 LGAs by revenue_per_active_listing
-- =========================
, ranked AS (
  SELECT lga_name, revenue_per_active_listing, median_age, avg_household_size,
         RANK() OVER (ORDER BY revenue_per_active_listing DESC) AS rk_desc,
         RANK() OVER (ORDER BY revenue_per_active_listing ASC)  AS rk_asc
  FROM lga_enriched
),
top13 AS (
  SELECT * FROM ranked WHERE rk_desc <= 13
),
bottom13 AS (
  SELECT * FROM ranked WHERE rk_asc  <= 13
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
FROM bottom13
;

-- =========================
-- Q2: Correlation between median age (Census) and revenue per active listing
-- =========================
SELECT
  corr(median_age, revenue_per_active_listing) AS corr_medianage_revenue
FROM lga_enriched
WHERE median_age IS NOT NULL;

-- =========================
-- Q3: Best type (property_type, room_type, accommodates) for top 15 neighbourhoods
--     by estimated revenue per active listing (last 12 months)
-- =========================
WITH nb_rev AS (
  SELECT
    listing_neighbourhood,
    SUM(estimated_revenue) / NULLIF(SUM(CASE WHEN has_availability THEN 1 ELSE 0 END),0) AS rev_per_active
  FROM last_12
  GROUP BY listing_neighbourhood
),
top15_nb AS (
  SELECT listing_neighbourhood
  FROM nb_rev
  ORDER BY rev_per_active DESC
  LIMIT 15
),
combo AS (
  SELECT
    f.listing_neighbourhood,
    f.property_type, f.room_type, f.accommodates,
    SUM(f.stays) AS total_stays
  FROM last_12 f
  JOIN top15_nb t USING (listing_neighbourhood)
  GROUP BY 1,2,3,4
)
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY total_stays DESC) AS rn
  FROM combo
) s
WHERE rn = 1
ORDER BY listing_neighbourhood;

-- =========================
-- Q4: Hosts with multiple listings in VIC:
--     Are their properties concentrated in the same LGA or distributed?
-- =========================
-- (Assumes we can detect VIC by LGA name containing '(VIC)' or similar; adjust filter.)
WITH host_props AS (
  SELECT DISTINCT host_id, lga_name
  FROM base
  WHERE lga_name ILIKE '%(VIC)%'
),
host_counts AS (
  SELECT host_id, COUNT(*) AS distinct_lgas
  FROM host_props
  GROUP BY host_id
)
SELECT
  SUM(CASE WHEN distinct_lgas = 1 THEN 1 ELSE 0 END) AS hosts_concentrated_one_lga,
  SUM(CASE WHEN distinct_lgas > 1 THEN 1 ELSE 0 END) AS hosts_spread_multiple_lgas
FROM host_counts;

-- =========================
-- Q5: Single-listing hosts in VIC: does revenue over last 12 months
--     cover the annualised median mortgage repayment in the LGA?
-- =========================
-- NOTE: Replace 'median_mortgage_repayment_monthly' with the real key from census_g02.
WITH vic_base AS (
  SELECT * FROM last_12 WHERE lga_name ILIKE '%(VIC)%'
),
host_one_listing AS (
  SELECT host_id
  FROM vic_base
  GROUP BY host_id
  HAVING COUNT(DISTINCT listing_id) = 1
),
host_revenue AS (
  SELECT host_id, SUM(estimated_revenue) AS rev_last_12m
  FROM vic_base
  WHERE host_id IN (SELECT host_id FROM host_one_listing)
  GROUP BY host_id
),
lga_mortgage AS (
  SELECT
    TRIM(raw->>'lga_name') AS lga_name,
    (raw->>'median_mortgage_repayment_monthly')::NUMERIC AS median_mortgage_monthly
  FROM silver.census_g02
  WHERE raw ? 'median_mortgage_repayment_monthly'
),
host_lga AS (
  SELECT DISTINCT b.host_id, b.lga_name
  FROM vic_base b
),
joined AS (
  SELECT
    h.host_id, h.rev_last_12m, lg.lga_name,
    lg.median_mortgage_monthly * 12 AS annual_mortgage
  FROM host_revenue h
  JOIN host_lga lg_host USING (host_id)
  JOIN lga_mortgage lg ON lg.lga_name = lg_host.lga_name
)
SELECT
  lga_name,
  COUNT(*) FILTER (WHERE rev_last_12m >= annual_mortgage) AS hosts_covering_mortgage,
  COUNT(*) AS total_single_listing_hosts,
  100.0 * COUNT(*) FILTER (WHERE rev_last_12m >= annual_mortgage) / NULLIF(COUNT(*),0) AS pct_covering
FROM joined
GROUP BY lga_name
ORDER BY pct_covering DESC NULLS LAST;