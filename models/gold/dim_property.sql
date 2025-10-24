{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

with s as (
  select listing_id, max(year_month) as last_month
  from {{ ref('listings_base') }}
  group by 1
),
latest as (
  select b.*
  from {{ ref('listings_base') }} b
  join s on s.listing_id = b.listing_id and s.last_month = b.year_month
)

select
  listing_id,
  lower(listing_neighbourhood)          as listing_neighbourhood,
  property_type,
  room_type,
  accommodates
from latest