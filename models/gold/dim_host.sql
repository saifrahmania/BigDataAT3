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
        -- ✅ Safe numeric conversion
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

        -- ✅ Clean text
        nullif(trim("HOST_NAME"),'') as host_name,
        nullif(trim("HOST_SINCE"),'') as host_since_raw,

        -- ✅ Parse date variants
        coalesce(
            to_date("HOST_SINCE", 'DD/MM/YYYY'),
            to_date("HOST_SINCE", 'YYYY-MM-DD')
        ) as host_since,

        -- ✅ Boolean normalization
        case
            when lower(coalesce("HOST_IS_SUPERHOST",'')) in ('t','true','yes','y','1')
            then true else false
        end as host_is_superhost,

        lower(nullif(trim("HOST_NEIGHBOURHOOD"),''))    as host_neighbourhood,
        lower(nullif(trim("LISTING_NEIGHBOURHOOD"),'')) as listing_neighbourhood,
        nullif(trim("PROPERTY_TYPE"),'')                as property_type,
        nullif(trim("ROOM_TYPE"),'')                    as room_type,
        cast("ACCOMMODATES" as int)                     as accommodates,

        -- ✅ Handle price robustly
        case
            when trim(cast("PRICE" as text)) ~ '^[0-9,.]+$'
            then cast(replace(cast("PRICE" as text), ',', '') as numeric)
            else null
        end as price,

        -- ✅ Availability flags
        case
            when lower(coalesce("HAS_AVAILABILITY",'')) in ('t','true','yes','y','1')
            then true else false
        end as has_availability,

        cast(coalesce("AVAILABILITY_30",0) as int)   as availability_30,
        cast(coalesce("NUMBER_OF_REVIEWS",0) as int) as number_of_reviews,

        -- ✅ Review rating safe cast
        cast(nullif(trim(cast("REVIEW_SCORES_RATING" as text)),'') as numeric)
            as review_scores_rating,

        -- ✅ Safe flag for price existence (no bigint coercion!)
        case
            when trim(cast("PRICE" as text)) <> '' and cast("PRICE" as text) is not null
            then true
            else false
        end as has_price,

        -- ✅ Derived temporal info
        '{{ t }}' as month_label,
        to_date(replace('{{ t }}','m',''),'MM_YYYY') as year_month

    from {{ source('bronze', t) }}

    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from base