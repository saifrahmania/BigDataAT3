{% snapshot property_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='listing_id',
    strategy='timestamp',
    updated_at='year_month'
  )
}}

select
  listing_id,
  property_type,
  room_type,
  accommodates,
  price,
  year_month
from {{ ref('listings_base') }}
where listing_id is not null

{% endsnapshot %}