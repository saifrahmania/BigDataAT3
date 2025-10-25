{% snapshot lga_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='lga_code_2016',
    strategy='check',
    check_cols=['lga_name']
  )
}}

select
  "LGA_CODE_2016" as lga_code_2016,
  "LGA_NAME" as lga_name
from {{ source('bronze', 'nsw_lga_suburb') }}

{% endsnapshot %}