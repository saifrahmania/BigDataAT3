{{ config(
    materialized='view',
    schema='dbt_mrahman_silver'
) }}

with b as (
    select *
    from {{ ref('listings_base') }}
),

monthly as (
    select
        listing_id,
        host_id,
        lower(coalesce(listing_neighbourhood, host_neighbourhood)) as listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price::numeric                                             as price,
        has_availability,
        availability_30::int                                       as availability_30,
        number_of_reviews::int                                     as number_of_reviews,
        -- estimated monthly revenue if active: nights_booked * price
        case
            when has_availability then greatest(0, 30 - coalesce(availability_30,0))::int * coalesce(price::numeric,0)
            else 0
        end                                                        as estimated_revenue,
        year_month
    from b
)

select * from monthly;