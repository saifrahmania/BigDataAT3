{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

select
  year_month,
  property_type,
  count(*)                               as listings,
  avg(nullif(price,0))                   as avg_price
from {{ ref('listings_monthly') }}
group by 1,2
order by 1,2