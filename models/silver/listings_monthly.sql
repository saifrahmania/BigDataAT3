{{ config(
    materialized = 'table',
    schema = 'dbt_mrahman_silver',
    alias = 'listings_monthly'
) }}

with base as (
    select * from {{ ref('listings_base') }}
),

month_extracted as (
    select
        listing_id,
        host_id,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        availability_30,
        is_active,
        number_of_stays,
        estimated_revenue,
        left(last_scraped, 7) as year_month
    from base
)

select
    listing_id,
    host_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    availability_30,
    is_active,
    number_of_stays,
    estimated_revenue,
    year_month
from month_extracted;