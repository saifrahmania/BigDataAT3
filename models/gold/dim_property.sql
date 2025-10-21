select distinct
  listing_id,
  lower(property_type) as property_type,
  lower(room_type) as room_type,
  accommodates
from {{ ref('listings_base') }}
where listing_id is not null
