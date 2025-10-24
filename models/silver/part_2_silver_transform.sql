{{ config(
    materialized='view',
    schema='dbt_mrahman_silver'
) }}

-- Combine Airbnb listings with LGA and Census data
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
        number_of_reviews::int,
        coalesce(lower(listing_neighbourhood), lower(host_neighbourhood)) as neighbourhood,
        year_month
    from {{ ref('listings_base') }}
    where price is not null
),
lga_map as (
    select
        lower(suburb) as suburb,
        lga_name,
        lga_code
    from bronze.nsw_lga_suburb
),
census_g01 as (
    select
        lga_code,
        median_age::int,
        median_mortgage_repay_monthly::numeric,
        median_rent_weekly::numeric
    from bronze."2016census_g01_nsw_lga"
),
census_g02 as (
    select
        lga_code,
        avg_household_size::numeric,
        median_tot_prsnl_inc_weekly::numeric
    from bronze."2016census_g02_nsw_lga"
)
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
    l.lga_code,
    c1.median_age,
    c1.median_mortgage_repay_monthly,
    c1.median_rent_weekly,
    c2.avg_household_size,
    c2.median_tot_prsnl_inc_weekly
from airbnb_clean a
left join lga_map l 
    on lower(a.neighbourhood) = lower(l.suburb)
left join census_g01 c1 
    on l.lga_code = c1.lga_code
left join census_g02 c2 
    on l.lga_code = c2.lga_code;