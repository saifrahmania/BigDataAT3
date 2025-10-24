{{ config(
    materialized='table',
    schema='dbt_mrahman_gold'
) }}

with base as (
  select * from {{ ref('listings_base') }}
),
lga_map as (
  select
    lower(suburb_name) as suburb_name,
    lower(lga_name)    as lga_name
  from {{ source('bronze','nsw_lga_suburb') }}
),
f as (
  select
    b.host_id,
    coalesce(l.lga_name, lower(b.host_neighbourhood)) as host_neighbourhood_lga,
    b.year_month,
    case when b.has_availability then
      greatest(0, 30 - coalesce(b.availability_30,0))::int * coalesce(b.price::numeric,0)
    else 0 end as estimated_revenue_when_active
  from base b
  left join lga_map l
    on l.suburb_name = lower(b.host_neighbourhood)
),
agg as (
  select
    host_neighbourhood_lga,
    year_month,
    count(distinct host_id)                       as distinct_hosts,
    sum(estimated_revenue_when_active)            as total_estimated_revenue_active,
    case when count(distinct host_id) = 0 then 0
         else sum(estimated_revenue_when_active)::numeric / count(distinct host_id)
    end                                           as estimated_revenue_per_host
  from f
  group by 1,2
)
select
  host_neighbourhood_lga,
  year_month,
  distinct_hosts,
  total_estimated_revenue_active,
  estimated_revenue_per_host
from agg
order by host_neighbourhood_lga, year_month