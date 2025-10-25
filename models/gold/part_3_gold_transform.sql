{{ config(materialized='view', schema='dbt_mrahman_gold') }}

with base as (
    select * from {{ ref('part_2_silver_transform') }}
),

dim_host as (
    select
        host_id,
        max(host_name) as host_name,
        bool_or(host_is_superhost) as host_is_superhost,
        count(distinct listing_id) as total_listings
    from base
    group by host_id
),

dim_property as (
    select
        property_type,
        room_type,
        round(avg(price), 2) as avg_price,
        round(avg(accommodates), 1) as avg_capacity
    from base
    group by property_type, room_type
),

fact_listing as (
    select
        listing_id,
        host_id,
        lga_name,
        price,
        number_of_reviews,
        median_age,
        median_rent_weekly,
        median_tot_prsnl_inc_weekly
    from base
)

select
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
from fact_listing f
LEFT JOIN dim_property p
  ON f.property_type = p.property_type
  AND f.room_type = p.room_type
LEFT JOIN dim_host h
  ON f.host_id = h.host_id