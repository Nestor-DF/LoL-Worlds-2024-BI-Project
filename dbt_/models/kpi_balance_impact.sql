{{ config(materialized='table') }}

with meta as (
    select * from {{ ref('kpi_champions_meta') }}
),

changes as (
    select * from {{ ref('stg_champion_last_changed') }}
),

tournament as (
    select 14.18 as tournament_version
),

joined as (
  select
    m.*,
    c.last_changed_raw,
    c.last_changed_version,
    t.tournament_version,
    t.tournament_version - c.last_changed_version as version_gap
  from meta m
  left join changes c
    on lower(m.champion) = lower(c.champion)
  cross join tournament t
)

select
  champion,
  picks,
  bans,
  wins,
  losses,
  win_rate_calc,
  presence_calc,
  last_changed_raw,
  last_changed_version,
  version_gap,
  case
    when version_gap is null then 'unknown'
    when version_gap <= 0.02 then 'recent'
    else 'old'
  end as balance_age_bucket
from joined
