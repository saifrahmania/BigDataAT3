{{ config(
    materialized = 'view',
    schema = 'dbt_mrahman_silver'
) }}

with base as (

    {% set months = [
        'm05_2020','m06_2020','m07_2020','m08_2020',
        'm09_2020','m10_2020','m11_2020','m12_2020',
        'm01_2021','m02_2021','m03_2021','m04_2021'
    ] %}

    {% for t in months %}
    select
        -- Listing & Host identifiers
        CASE
            WHEN trim(cast("LISTING_ID" as text)) ~ '^[0-9]+$'
                THEN cast("LISTING_ID" as bigint)
            ELSE NULL
        END AS listing_id,

        CASE
            WHEN trim(cast("HOST_ID" as text)) ~ '^[0-9]+$'
                THEN cast("HOST_ID" as bigint)
            ELSE NULL
        END AS host_id,

        nullif(trim("HOST_NAME"),'') as host_name,
        nullif(trim("HOST_SINCE"),'') as host_since_raw,

        -- Parse host_since dates (supports DD/MM/YYYY and YYYY-MM-DD)
        coalesce(
            to_date("HOST_SINCE",'DD/MM/YYYY'),
            to_date("HOST_SINCE",'YYYY-MM-DD')
        ) as host_since,

        CASE
            WHEN lower(coalesce("HOST_IS_SUPERHOST",'')) IN ('t','true','yes','y','1')
                THEN true ELSE false
        END AS host_is_superhost,

        lower(nullif(trim("HOST_NEIGHBOURHOOD"),''))  as host_neighbourhood,
        lower(nullif(trim("LISTING_NEIGHBOURHOOD"),'')) as listing_neighbourhood,
        nullif(trim("PROPERTY_TYPE"),'')              as property_type,
        nullif(trim("ROOM_TYPE"),'')                  as room_type,
        cast("ACCOMMODATES" as int)                   as accommodates,

        -- ✅ Safe PRICE handling (fixes bigint/empty string issue)
        CASE
            WHEN trim(cast("PRICE" as text)) = '' THEN NULL
            ELSE cast(NULLIF(regexp_replace(cast("PRICE" as text), '[^0-9.]', '', 'g'), '') as numeric)
        END AS price,

        CASE
            WHEN lower(coalesce("HAS_AVAILABILITY",'')) IN ('t','true','yes','y','1')
                THEN true ELSE false
        END AS has_availability,

        cast(coalesce("AVAILABILITY_30",0) as int)    as availability_30,
        cast(coalesce("NUMBER_OF_REVIEWS",0) as int)  as number_of_reviews,

        -- Review score cleanup
        cast(NULLIF(trim(cast("REVIEW_SCORES_RATING" as text)),'') as numeric) as review_scores_rating,
        cast(NULLIF(trim(cast("REVIEW_SCORES_ACCURACY" as text)),'') as numeric) as review_scores_accuracy,
        cast(NULLIF(trim(cast("REVIEW_SCORES_CLEANLINESS" as text)),'') as numeric) as review_scores_cleanliness,
        cast(NULLIF(trim(cast("REVIEW_SCORES_CHECKIN" as text)),'') as numeric) as review_scores_checkin,
        cast(NULLIF(trim(cast("REVIEW_SCORES_COMMUNICATION" as text)),'') as numeric) as review_scores_communication,
        cast(NULLIF(trim(cast("REVIEW_SCORES_VALUE" as text)),'') as numeric) as review_scores_value,

        -- Derived flag: whether price exists
        CASE
            WHEN NULLIF(trim(cast("PRICE" as text)),'') IS NULL THEN false
            ELSE true
        END AS has_price,

        -- Month label
        '{{ t }}' as month_label,
        to_date(replace('{{ t }}','m',''),'MM_YYYY') as year_month

    from {{ source('bronze', t) }}

    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}

)

select * from base