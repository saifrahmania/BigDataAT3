{{ config(
    materialized='view',
    schema='dbt_mrahman_silver'
) }}

-- STEP 1: Clean Airbnb listing data from listings_base
with airbnb_clean as (
    select
        listing_id,
        host_id,
        host_name,
        host_is_superhost,
        property_type,
        room_type,
        accommodates,
        price::numeric as price,
        number_of_reviews::int as number_of_reviews,
        coalesce(lower(listing_neighbourhood), lower(host_neighbourhood)) as neighbourhood,
        year_month
    from {{ ref('listings_base') }}
    where price is not null
),

-- STEP 2: LGA mapping data (bronze.nsw_lga_suburb)
lga_map as (
    select
        lower("SUBURB_NAME") as suburb_name,
        "LGA_NAME" as lga_name
    from bronze.nsw_lga_suburb
),

-- STEP 3: Census G01 (demographic data)
census_g01 as (
    select
        "LGA_CODE_2016" as lga_code,
        "Tot_P_P"::bigint as total_population
    from bronze."2016census_g01_nsw_lga"
),

-- STEP 4: Census G02 (median & household data)
census_g02 as (
    select
        "LGA_CODE_2016" as lga_code,
        "Median_age_persons"::int as median_age,
        "Median_mortgage_repay_monthly"::numeric as median_mortgage_repay_monthly,
        "Median_rent_weekly"::numeric as median_rent_weekly,
        "Median_tot_prsnl_inc_weekly"::numeric as median_tot_prsnl_inc_weekly,
        "Average_household_size"::numeric as avg_household_size
    from bronze."2016census_g02_nsw_lga"
)

-- STEP 5: Join all datasets
select
    a.listing_id,
    a.host_id,
    a.host_name,
    a.host_is_superhost,
    a.property_type,
    a.room_type,
    a.accommodates,
    a.price,
    a.number_of_reviews,
    a.neighbourhood,
    a.year_month,
    l.lga_name,
    c1.total_population,
    c2.median_age,
    c2.median_mortgage_repay_monthly,
    c2.median_rent_weekly,
    c2.median_tot_prsnl_inc_weekly,
    c2.avg_household_size
from airbnb_clean a
left join lga_map l
    on lower(a.neighbourhood) = l.suburb_name
left join census_g01 c1
    on l.lga_name = c1.lga_code
left join census_g02 c2
    on l.lga_name = c2.lga_code