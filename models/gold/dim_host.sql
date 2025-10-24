{{ config(
    materialized='view',
    schema='dbt_mrahman_silver'
) }}

with base as (

    {% set months = [
        'm05_2020','m06_2020','m07_2020','m08_2020',
        'm09_2020','m10_2020','m11_2020','m12_2020',
        'm01_2021','m02_2021','m03_2021','m04_2021'
    ] %}

    {% for t in months %}
    select
        "LISTING_ID"::bigint as listing_id,
        "HOST_ID"::bigint as host_id,
        nullif(trim(cast("HOST_NAME" as text)), '') as host_name,
        nullif(trim(cast("HOST_SINCE" as text)), '') as host_since_raw,

        coalesce(
            to_date(trim(cast("HOST_SINCE" as text)), 'DD/MM/YYYY'),
            to_date(trim(cast("HOST_SINCE" as text)), 'YYYY-MM-DD')
        ) as host_since,

        case
            when lower(coalesce(trim(cast("HOST_IS_SUPERHOST" as text)), '')) in ('t','true','yes','y','1')
            then true else false
        end as host_is_superhost,

        lower(nullif(trim(cast("HOST_NEIGHBOURHOOD" as text)), '')) as host_neighbourhood,
        lower(nullif(trim(cast("LISTING_NEIGHBOURHOOD" as text)), '')) as listing_neighbourhood,
        nullif(trim(cast("PROPERTY_TYPE" as text)), '') as property_type,
        nullif(trim(cast("ROOM_TYPE" as text)), '') as room_type,
        "ACCOMMODATES"::int as accommodates,

        -- ✅ PRICE is already bigint, just cast safely to numeric
        "PRICE"::numeric as price,

        case
            when lower(coalesce(trim(cast("HAS_AVAILABILITY" as text)), '')) in ('t','true','yes','y','1')
            then true else false
        end as has_availability,

        coalesce("AVAILABILITY_30", 0)::int as availability_30,
        coalesce("NUMBER_OF_REVIEWS", 0)::int as number_of_reviews,
        cast("REVIEW_SCORES_RATING" as numeric) as review_scores_rating,

        -- ✅ safer: just check for not null
        (case when "PRICE" is not null then true else false end) as has_price,

        '{{ t }}' as month_label,
        to_date(replace('{{ t }}','m',''), 'MM_YYYY') as year_month

    from {{ source('bronze', t) }}

    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from base;