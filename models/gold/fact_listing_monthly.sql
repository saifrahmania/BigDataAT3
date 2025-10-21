with f as (
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
  from {{ ref('listing_monthly') }}
)
select * from f
