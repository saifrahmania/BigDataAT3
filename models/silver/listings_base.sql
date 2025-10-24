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
        cast(listing_id as bigint)                               as listing_id,
        cast(host_id as bigint)                                  as host_id,
        nullif(trim(host_name),'')                               as host_name,
        nullif(trim(host_since),'')                              as host_since_raw,
        -- Try to coerce day/month/year and year-month-day patterns
        coalesce(
            to_date(host_since, 'DD/MM/YYYY'),
            to_date(host_since, 'YYYY-MM-DD')
        )                                                        as host_since,
        case when lower(coalesce(host_is_superhost,'')) in ('t','true','yes','y','1') then true else false end as host_is_superhost,
        lower(nullif(trim(host_neighbourhood),''))               as host_neighbourhood,
        lower(nullif(trim(listing_neighbourhood),''))            as listing_neighbourhood,
        nullif(trim(property_type),'')                           as property_type,
        nullif(trim(room_type),'')                               as room_type,
        cast(accommodates as int)                                as accommodates,
        cast(replace(price, ',', '') as numeric)                 as price,
        case when lower(coalesce(has_availability,'')) in ('t','true','yes','y','1') then true else false end as has_availability,
        cast(coalesce(availability_30,0) as int)                 as availability_30,
        cast(coalesce(number_of_reviews,0) as int)               as number_of_reviews,
        cast(nullif(review_scores_rating,'') as numeric)         as review_scores_rating,

        -- Pre-calc handy flags
        (case when price is not null and price <> '' then true else false end) as has_price,

        -- Derive year_month label from table name
        '{{ t[1:3] }}_20{{ t[4:6] }}'                            as month_label,   -- e.g. 05_2020
        to_date('{{ t[1:3] }}_20{{ t[4:6] }}','MM_YYYY')         as year_month

    from {{ source('bronze', t) }}

    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from base