{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

with s as (
  select
    host_id,
    max(year_month) as last_month
  from {{ ref('listings_base') }}
  group by 1
),
latest as (
  select b.*
  from {{ ref('listings_base') }} b
  join s on s.host_id = b.host_id and s.last_month = b.year_month
)

select
  host_id,
  max(host_name)                        as host_name,
  max(host_since)                       as host_since,
  max(host_is_superhost)::boolean       as host_is_superhost,
  max(host_neighbourhood)               as host_neighbourhood
from latest
group by host_id;