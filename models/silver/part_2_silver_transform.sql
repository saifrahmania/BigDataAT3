{{ config(
    materialized='view',
    schema='dbt_mrahman_silver'
) }}

-- Minimal, guaranteed-to-compile transform using only known columns from listings_base
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
)

select * from airbnb_clean;