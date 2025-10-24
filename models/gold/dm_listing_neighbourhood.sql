{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

select
  year_month,
  lower(listing_neighbourhood)             as listing_neighbourhood,
  count(*)                                 as listings,
  avg(nullif(price,0))                     as avg_price,
  sum(estimated_revenue)                   as total_estimated_revenue
from {{ ref('listings_monthly') }}
group by 1,2
order by 1,2;