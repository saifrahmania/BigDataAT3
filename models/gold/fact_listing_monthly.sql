{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

with f as (
  select
    listing_id,
    host_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    estimated_revenue,
    year_month
  from {{ ref('listings_monthly') }}
)
select * from f;