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
        cast("LISTING_ID" as bigint)                            as listing_id,
        cast("HOST_ID" as bigint)                               as host_id,
        nullif(trim("HOST_NAME"),'')                            as host_name,
        nullif(trim("HOST_SINCE"),'')                           as host_since_raw,

        -- Try to coerce day/month/year and year-month-day patterns
        coalesce(
            to_date("HOST_SINCE", 'DD/MM/YYYY'),
            to_date("HOST_SINCE", 'YYYY-MM-DD')
        )                                                       as host_since,

        case when lower(coalesce("HOST_IS_SUPERHOST",'')) in ('t','true','yes','y','1') 
             then true else false end                           as host_is_superhost,

        lower(nullif(trim("HOST_NEIGHBOURHOOD"),''))            as host_neighbourhood,
        lower(nullif(trim("LISTING_NEIGHBOURHOOD"),''))         as listing_neighbourhood,
        nullif(trim("PROPERTY_TYPE"),'')                        as property_type,
        nullif(trim("ROOM_TYPE"),'')                            as room_type,
        cast("ACCOMMODATES" as int)                             as accommodates,

        -- Fix 1: ensure PRICE is text before replace()
        cast(replace(cast("PRICE" as text), ',', '') as numeric) as price,

        case when lower(coalesce("HAS_AVAILABILITY",'')) in ('t','true','yes','y','1') 
             then true else false end                           as has_availability,

        cast(coalesce("AVAILABILITY_30",0) as int)              as availability_30,
        cast(coalesce("NUMBER_OF_REVIEWS",0) as int)            as number_of_reviews,

        -- ✅ Fix 2: safely handle empty REVIEW_SCORES_RATING
        cast(nullif(trim(cast("REVIEW_SCORES_RATING" as text)), '') as numeric)
                                                                as review_scores_rating,

        -- Handy derived flags
        (case when "PRICE" is not null and "PRICE" <> '' then true else false end) as has_price,

        -- Month identifier
        '{{ t }}'                                               as month_label,
        to_date(replace('{{ t }}','m',''),'MM_YYYY')            as year_month

    from {{ source('bronze', t) }}

    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from base