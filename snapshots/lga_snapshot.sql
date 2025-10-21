{% snapshot lga_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='lga_code',    -- or proper PK in your mapping
    strategy='timestamp',
    updated_at='year_month'   -- if you don’t have a month in this ref data, switch to 'check' strategy
  )
}}
select
  lga_code,
  lga_name,
  null::date as year_month  -- if no month, consider using check strategy instead
from {{ source('bronze','nsw_lga_code') }}
{% endsnapshot %}
