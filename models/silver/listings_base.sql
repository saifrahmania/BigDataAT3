with raw as (
  -- UNION all monthly tables using the declared var list
  {% set months = var('airbnb_months') %}
  {% for m in months %}
  select
    -- Keep raw identifiers in a consistent set; tweak these to your CSV headers if needed
    cast(listing_id as bigint) as listing_id,
    cast(host_id as bigint) as host_id,
    lower(nullif(trim(host_name), '')) as host_name,
    case when host_is_superhost in ('t','true','1') then true else false end as is_superhost,
    lower(nullif(trim(host_neighbourhood), '')) as host_neighbourhood,
    lower(nullif(trim(listing_neighbourhood), '')) as listing_neighbourhood,
    lower(nullif(trim(property_type), '')) as property_type,
    lower(nullif(trim(room_type), '')) as room_type,
    cast(nullif(accommodates,'') as int) as accommodates,
    cast(replace(nullif(price,''), '$','') as numeric) as price,
    cast(nullif(availability_30,'') as int) as availability_30,
    cast(nullif(number_of_reviews,'') as int) as number_of_reviews,
    cast(nullif(review_scores_rating,'') as numeric) as review_scores_rating,
    cast(nullif(review_scores_accuracy,'') as numeric) as review_scores_accuracy,
    cast(nullif(review_scores_cleanliness,'') as numeric) as review_scores_cleanliness,
    cast(nullif(review_scores_checkin,'') as numeric) as review_scores_checkin,
    cast(nullif(review_scores_communication,'') as numeric) as review_scores_communication,
    cast(nullif(review_scores_value,'') as numeric) as review_scores_value,
    -- assumes a column like "has_availability" exists as 't'/'f'
    case when has_availability in ('t','true','1') then true else false end as has_availability,
    -- month derived from file partition; pass as literal
    to_date('{{ m }}','MM_YYYY') as year_month
  from {{ source('bronze', m | lower) }}
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
),
dedup as (
  select *,
         row_number() over (partition by listing_id, year_month order by listing_id) as rn
  from raw
)
select
  listing_id,
  host_id,
  host_name,
  is_superhost,
  host_neighbourhood,
  listing_neighbourhood,
  property_type,
  room_type,
  accommodates,
  price,
  availability_30,
  number_of_reviews,
  review_scores_rating,
  review_scores_accuracy,
  review_scores_cleanliness,
  review_scores_checkin,
  review_scores_communication,
  review_scores_value,
  has_availability,
  year_month
from dedup
where rn = 1
