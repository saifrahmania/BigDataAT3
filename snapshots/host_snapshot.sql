{% snapshot host_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='host_id',
    strategy='timestamp',
    updated_at='year_month'
  )
}}

select
  host_id,
  host_name,
  host_is_superhost,
  host_neighbourhood,
  year_month
from {{ ref('silver.listings_base') }}
where host_id is not null

{% endsnapshot %}