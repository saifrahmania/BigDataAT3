with f as (select * from {{ ref('fact_listing_monthly') }}),

agg as (
  select
    lower(property_type) as property_type,
    lower(room_type) as room_type,
    accommodates,
    year_month,
    count(*)::numeric as total_listings,
    sum(is_active)::numeric as active_listings,
    case when count(*)=0 then 0 else sum(is_active)::numeric / count(*) end as active_listings_rate,
    min(case when is_active=1 then price end) as min_price_active,
    max(case when is_active=1 then price end) as max_price_active,
    percentile_cont(0.5) within group (order by case when is_active=1 then price end) as median_price_active,
    avg(case when is_active=1 then price end) as avg_price_active,
    count(distinct host_id) as distinct_hosts,
    avg(case when is_active=1 then nullif(review_scores_rating,0) end) as avg_review_score_active,
    sum(case when is_active=1 then number_of_stays else 0 end) as total_stays_active,
    case when sum(case when is_active=1 then 1 else 0 end)=0
         then 0
         else sum(case when is_active=1 then estimated_revenue else 0 end)
              / nullif(sum(case when is_active=1 then 1 else 0 end),0)
    end as avg_estimated_revenue_per_active_listing
  from f
  group by 1,2,3,4
),
chg as (
  select
    a.*,
    lag(active_listings) over (partition by property_type, room_type, accommodates order by year_month) as prev_active,
    lag(total_listings)  over (partition by property_type, room_type, accommodates order by year_month) as prev_total
  from agg a
)
select
  property_type, room_type, accommodates, year_month,
  active_listings_rate,
  min_price_active, max_price_active, median_price_active, avg_price_active,
  distinct_hosts,
  avg_review_score_active,
  case when prev_active is null or prev_active=0 then null
       else (active_listings - prev_active)::numeric / nullif(prev_active,0)
  end as pct_change_active_listings,
  case when prev_total is null or prev_total=0 then null
       else (total_listings - prev_total)::numeric / nullif(prev_total,0)
  end as pct_change_inactive_listings,  -- if you need inactive, compute with (total-active)
  total_stays_active as total_number_of_stays,
  avg_estimated_revenue_per_active_listing
from chg
order by property_type, room_type, accommodates, year_month
