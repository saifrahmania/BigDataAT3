{% snapshot lga_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='lga_code',
    strategy='timestamp',
    updated_at='year_month'
  )
}}

select
  lga_code,
  lga_name,
  null::date as year_month
from {{ source('census', 'nsw_lga_code') }}

{% endsnapshot %}
