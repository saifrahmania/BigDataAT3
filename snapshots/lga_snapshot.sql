{% snapshot lga_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='lga_code_2016',
    strategy='timestamp',
    updated_at='year_month'
  )
}}

select
  "LGA_CODE_2016" as lga_code_2016,
  null::varchar as lga_name,
  null::date as year_month
from bronze."2016census_g01_nsw_lga"

{% endsnapshot %}