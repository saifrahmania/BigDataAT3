{{ config(
    materialized = 'view',
    schema = 'dbt_mrahman_silver',
    alias = 'listings_base'
) }}

with airbnb_raw as (

    select * from {{ source('bronze', 'm05_2020') }}
    union all
    select * from {{ source('bronze', 'm06_2020') }}
    union all
    select * from {{ source('bronze', 'm07_2020') }}
    union all
    select * from {{ source('bronze', 'm08_2020') }}
    union all
    select * from {{ source('bronze', 'm09_2020') }}
    union all
    select * from {{ source('bronze', 'm10_2020') }}
    union all
    select * from {{ source('bronze', 'm11_2020') }}
    union all
    select * from {{ source('bronze', 'm12_2020') }}
    union all
    select * from {{ source('bronze', 'm01_2021') }}
    union all
    select * from {{ source('bronze', 'm02_2021') }}
    union all
    select * from {{ source('bronze', 'm03_2021') }}
    union all
    select * from {{ source('bronze', 'm04_2021') }}

),

renamed as (
    select
        id as listing_id,
        host_id,
        neighbourhood_cleansed as listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        availability_30,
        case when availability_30 < 30 then true else false end as is_active,
        number_of_reviews as number_of_stays,
        calculated_host_listings_count as total_host_listings,
        (price * (30 - availability_30)) as estimated_revenue
    from airbnb_raw
)

select * from renamed;