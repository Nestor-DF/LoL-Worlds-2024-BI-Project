{{ config(materialized='table') }}

with c as (
  select * from {{ ref('stg_champions_stats') }}
),

totals as (
  -- total de partidas ≈ picks totales / 10 (10 campeones por partida)
  select
    case when nullif(sum(c.picks), 0) is null then null
         else (sum(c.picks) * 1.0) / 10.0
    end as total_games
  from c
),

agg as (
  select
    champion,
    sum(picks)  as picks,
    sum(bans)   as bans,
    sum(wins)   as wins,
    sum(losses) as losses,
    (sum(picks) + sum(bans)) as contests,

    case when nullif(sum(picks), 0) is null then null
         else (sum(wins) * 1.0) / nullif(sum(picks), 0)
    end as win_rate_calc,

    avg(dmg_per_min)  as dmg_per_min_avg,
    avg(gold_per_min) as gold_per_min_avg,

    case when nullif(avg(gold_per_min), 0) is null then null
         else avg(dmg_per_min) / nullif(avg(gold_per_min), 0)
    end as dmg_per_gold_per_min_avg,

    avg(gold_diff_15) as gold_diff_15_avg,
    avg(cs_diff_15)   as cs_diff_15_avg,
    avg(xp_diff_15)   as xp_diff_15_avg
  from c
  group by champion
)

select
  a.*,
  t.total_games,

  case when nullif(t.total_games, 0) is null then null
       else (a.contests * 1.0) / nullif(t.total_games, 0)
  end as presence_calc,

  case when nullif(a.picks, 0) is null then null
       else (a.bans * 1.0) / nullif(a.picks, 0)
  end as ban_to_pick_ratio
from agg a
cross join totals t
