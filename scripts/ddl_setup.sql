-- ============================================================
-- DDL Setup for BigDataAT3 Airbnb & Census Pipeline
-- Includes: bronze, silver, and gold schemas with key tables
-- ============================================================

-- =====================
-- SCHEMA CREATION
-- =====================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- BRONZE TABLES
-- ============================================================

-- Raw Airbnb listings (seeded from CSV)
DROP TABLE IF EXISTS bronze.listings_raw;
CREATE TABLE bronze.listings_raw (
    LISTING_ID TEXT,
    HOST_ID TEXT,
    HOST_NAME TEXT,
    HOST_IS_SUPERHOST TEXT,
    HOST_NEIGHBOURHOOD TEXT,
    HOST_SINCE TEXT,
    LISTING_NEIGHBOURHOOD TEXT,
    PROPERTY_TYPE TEXT,
    ROOM_TYPE TEXT,
    ACCOMMODATES TEXT,
    AVAILABILITY_30 TEXT,
    HAS_AVAILABILITY TEXT,
    NUMBER_OF_REVIEWS TEXT,
    PRICE TEXT,
    REVIEW_SCORES_RATING TEXT,
    REVIEW_SCORES_ACCURACY TEXT,
    REVIEW_SCORES_CLEANLINESS TEXT,
    REVIEW_SCORES_CHECKIN TEXT,
    REVIEW_SCORES_COMMUNICATION TEXT,
    REVIEW_SCORES_VALUE TEXT,
    SCRAPE_ID TEXT,
    SCRAPED_DATE TEXT,
    YEAR_MONTH TEXT
);

-- Census G01 table (cleaned prototype)
DROP TABLE IF EXISTS bronze.census_g01_nsw_lga;
CREATE TABLE bronze.census_g01_nsw_lga (
    lga_code_2016 TEXT,
    lga_name TEXT,
    total_persons INTEGER,
    median_age INTEGER,
    average_household_size NUMERIC
);

-- Census G02 table (cleaned prototype)
DROP TABLE IF EXISTS bronze.census_g02_nsw_lga;
CREATE TABLE bronze.census_g02_nsw_lga (
    lga_code_2016 TEXT,
    lga_name TEXT,
    median_tot_prsnl_inc_weekly INTEGER,
    median_rent_weekly INTEGER,
    median_mortgage_monthly INTEGER,
    average_num_motor_vehicles_per_dwelling NUMERIC
);

-- NSW LGA code
DROP TABLE IF EXISTS bronze.nsw_lga_code;
CREATE TABLE bronze.nsw_lga_code (
    lga_code TEXT,
    lga_name TEXT
);

-- NSW LGA suburb mapping
DROP TABLE IF EXISTS bronze.nsw_lga_suburb;
CREATE TABLE bronze.nsw_lga_suburb (
    suburb_name TEXT,
    lga_name TEXT
);

-- ============================================================
-- SILVER TABLES
-- ============================================================

-- listings_base: cleaned + typed Airbnb listing data
DROP TABLE IF EXISTS silver.listings_base;
CREATE TABLE silver.listings_base (
    listing_id BIGINT,
    host_id BIGINT,
    host_name TEXT,
    host_is_superhost BOOLEAN,
    host_neighbourhood TEXT,
    host_since DATE,
    listing_neighbourhood TEXT,
    property_type TEXT,
    room_type TEXT,
    accommodates INTEGER,
    availability_30 INTEGER,
    has_availability BOOLEAN,
    number_of_reviews INTEGER,
    price NUMERIC(12,2),
    review_scores_rating NUMERIC(6,2),
    review_scores_accuracy NUMERIC(6,2),
    review_scores_cleanliness NUMERIC(6,2),
    review_scores_checkin NUMERIC(6,2),
    review_scores_communication NUMERIC(6,2),
    review_scores_value NUMERIC(6,2),
    scrape_id BIGINT,
    scraped_date DATE,
    year_month TEXT
);

-- Monthly rollup per listing (aggregated view)
DROP TABLE IF EXISTS silver.listing_monthly;
CREATE TABLE silver.listing_monthly (
    listing_id BIGINT,
    year_month TEXT,
    has_availability BOOLEAN,
    price NUMERIC(12,2),
    availability_30 INTEGER,
    number_of_reviews INTEGER,
    review_scores_rating NUMERIC(6,2),
    room_type TEXT,
    property_type TEXT,
    accommodates INTEGER,
    listing_neighbourhood TEXT,
    host_id BIGINT,
    stays INTEGER,
    estimated_revenue NUMERIC(14,2)
);

-- G01 census summary (silver)
DROP TABLE IF EXISTS silver.census_g01_silver;
CREATE TABLE silver.census_g01_silver AS
SELECT * FROM bronze.census_g01_nsw_lga;

-- G02 census summary (silver)
DROP TABLE IF EXISTS silver.census_g02_silver;
CREATE TABLE silver.census_g02_silver AS
SELECT * FROM bronze.census_g02_nsw_lga;

-- Suburb → LGA mapping (silver)
DROP TABLE IF EXISTS silver.nsw_lga_suburb_silver;
CREATE TABLE silver.nsw_lga_suburb_silver AS
SELECT * FROM bronze.nsw_lga_suburb;

-- LGA Code (silver)
DROP TABLE IF EXISTS silver.nsw_lga_code_silver;
CREATE TABLE silver.nsw_lga_code_silver AS
SELECT * FROM bronze.nsw_lga_code;

-- ============================================================
-- GOLD TABLES
-- ============================================================

-- Dim Host
DROP TABLE IF EXISTS gold.dim_host;
CREATE TABLE gold.dim_host (
    host_id BIGINT PRIMARY KEY,
    host_name TEXT,
    host_is_superhost BOOLEAN,
    host_since DATE,
    host_neighbourhood TEXT
);

-- Dim Property
DROP TABLE IF EXISTS gold.dim_property;
CREATE TABLE gold.dim_property (
    listing_id BIGINT PRIMARY KEY,
    room_type TEXT,
    property_type TEXT,
    accommodates INTEGER
);

-- Dim Listing Neighbourhood (optional LGA enrichment)
DROP TABLE IF EXISTS gold.dm_listing_neighbourhood;
CREATE TABLE gold.dm_listing_neighbourhood (
    listing_neighbourhood TEXT PRIMARY KEY,
    lga_name TEXT
);

-- Dim Host Neighbourhood (optional LGA enrichment)
DROP TABLE IF EXISTS gold.dm_host_neighbourhood;
CREATE TABLE gold.dm_host_neighbourhood (
    host_neighbourhood TEXT PRIMARY KEY,
    lga_name TEXT
);

-- Dim Property Type
DROP TABLE IF EXISTS gold.dm_property_type;
CREATE TABLE gold.dm_property_type (
    property_type TEXT PRIMARY KEY,
    is_entire_home BOOLEAN,
    is_shared_room BOOLEAN
);

-- Fact Table: Listing Monthly Snapshot
DROP TABLE IF EXISTS gold.fact_listing_monthly;
CREATE TABLE gold.fact_listing_monthly (
    listing_id BIGINT,
    year_month TEXT,
    has_availability BOOLEAN,
    price NUMERIC(12,2),
    availability_30 INTEGER,
    number_of_reviews INTEGER,
    review_scores_rating NUMERIC(6,2),
    stays INTEGER,
    estimated_revenue NUMERIC(14,2),
    lga_code TEXT
);
