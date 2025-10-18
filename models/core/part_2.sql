-- ==========================================
-- PART 2: Data Warehouse DDL (Silver + Gold)
-- Schemas: silver, gold
-- Tables based on your /silver_prototype and /gold_prototype CSVs
-- ==========================================

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- --------------------------
-- SILVER TABLES
-- --------------------------
-- listings_base (cleaned, typed, deduped one row per listing_id,year_month)
DROP TABLE IF EXISTS silver.listings_base CASCADE;
CREATE TABLE silver.listings_base (
  listing_id              BIGINT,
  host_id                 BIGINT,
  host_name               TEXT,
  host_is_superhost       BOOLEAN,
  host_neighbourhood      TEXT,
  host_since              DATE,
  listing_neighbourhood   TEXT,
  property_type           TEXT,
  room_type               TEXT,
  accommodates            INT,
  availability_30         INT,
  has_availability        BOOLEAN,
  number_of_reviews       INT,
  price                   NUMERIC(12,2),
  review_scores_rating            NUMERIC(6,2),
  review_scores_accuracy          NUMERIC(6,2),
  review_scores_cleanliness       NUMERIC(6,2),
  review_scores_checkin           NUMERIC(6,2),
  review_scores_communication     NUMERIC(6,2),
  review_scores_value             NUMERIC(6,2),
  scrape_id               BIGINT,
  scraped_date            DATE,
  year_month              TEXT
);

-- listing_monthly (future fact grain at listing_id,year_month)
DROP TABLE IF EXISTS silver.listing_monthly CASCADE;
CREATE TABLE silver.listing_monthly (
  listing_id            BIGINT,
  year_month            TEXT,
  has_availability      BOOLEAN,
  price                 NUMERIC(12,2),
  availability_30       INT,
  number_of_reviews     INT,
  review_scores_rating  NUMERIC(6,2),
  room_type             TEXT,
  property_type         TEXT,
  accommodates          INT,
  listing_neighbourhood TEXT,
  host_id               BIGINT,
  stays                 INT,
  estimated_revenue     NUMERIC(14,2)
);

-- Reference / lookup tables
DROP TABLE IF EXISTS silver.census_g01 CASCADE;
CREATE TABLE silver.census_g01 (
  -- keep flexible: load all columns as TEXT, cast in dbt models where needed
  -- You can ALTER COLUMN later for important measures.
  -- Below is the generic skeleton:
  -- Use: \copy silver.census_g01 FROM 'census_g01_silver.csv' CSV HEADER
  -- (columns will be added by COPY since we're loading as text into a staging table)
  -- For portability, create a single TEXT column and use CSV header mapping via COPY.
  -- If you prefer strict DDL, replace with explicit columns.
  -- Minimal structure:
  raw JSONB
);

DROP TABLE IF EXISTS silver.census_g02 CASCADE;
CREATE TABLE silver.census_g02 (
  raw JSONB
);

DROP TABLE IF EXISTS silver.nsw_lga_code CASCADE;
CREATE TABLE silver.nsw_lga_code (
  raw JSONB
);

DROP TABLE IF EXISTS silver.nsw_lga_suburb CASCADE;
CREATE TABLE silver.nsw_lga_suburb (
  raw JSONB
);

-- NOTE:
-- The 4 reference tables are set as JSONB staging to avoid column-name mismatches now.
-- In dbt, you will create typed views/models that extract fields from raw->>key.
-- If you prefer strict DDL here, replace the above with explicit columns based on your CSV headers.


-- --------------------------
-- GOLD TABLES (star schema)
-- --------------------------

-- Dimensions (from your gold_prototype)
DROP TABLE IF EXISTS gold.dim_host CASCADE;
CREATE TABLE gold.dim_host (
  host_id           BIGINT PRIMARY KEY,
  host_name         TEXT,
  host_is_superhost BOOLEAN,
  host_neighbourhood TEXT,
  host_since        DATE
);

DROP TABLE IF EXISTS gold.dim_property CASCADE;
CREATE TABLE gold.dim_property (
  listing_id     BIGINT PRIMARY KEY,
  property_type  TEXT,
  room_type      TEXT,
  accommodates   INT,
  price          NUMERIC(12,2)
);

-- Optional: neighbourhood/LGA dims (create later once you finalize mapping)
-- CREATE TABLE gold.dim_neighbourhood (...);
-- CREATE TABLE gold.dim_lga (...);

-- Fact
DROP TABLE IF EXISTS gold.fact_listing_monthly CASCADE;
CREATE TABLE gold.fact_listing_monthly (
  listing_id            BIGINT,
  year_month            TEXT,
  price                 NUMERIC(12,2),
  availability_30       INT,
  number_of_reviews     INT,
  stays                 INT,
  estimated_revenue     NUMERIC(14,2),
  host_id               BIGINT,
  listing_neighbourhood TEXT,
  property_type         TEXT,
  room_type             TEXT,
  accommodates          INT,
  has_availability      BOOLEAN,
  review_scores_rating  NUMERIC(6,2),
  PRIMARY KEY(listing_id, year_month)
);

-- Datamarts (create as views later in dbt; for now placeholders)
-- CREATE VIEW gold.dm_listing_neighbourhood AS ...;
-- CREATE VIEW gold.dm_property_type AS ...;
-- CREATE VIEW gold.dm_host_neighbourhood AS ...;