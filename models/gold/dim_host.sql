-- Latest version of host dimension (if using snapshots, this can be a view over snapshot latest rows)
with latest as (
  select distinct
    host_id,
    max(host_name) over (partition by host_id) as host_name,
    max(is_superhost::int) over (partition by host_id) = 1 as is_superhost,
    max(host_neighbourhood) over (partition by host_id) as host_neighbourhood
  from {{ ref('listings_base') }}
)
select distinct host_id, host_name, is_superhost, host_neighbourhood
from latest
