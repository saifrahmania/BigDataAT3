with base as (
  select * from {{ ref('listings_base') }}
),
calc as (
  select
    listing_id,
    host_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    availability_30,
    case when has_availability then 1 else 0 end as is_active,
    (greatest(0, 30 - coalesce(availability_30,0))) as number_of_stays,   -- assignment uses 30
    (greatest(0, 30 - coalesce(availability_30,0))) * coalesce(price,0) as estimated_revenue,
    year_month
  from base
)
select * from calc
