{{ config(materialized='table') }}

with p as (
  select * from {{ ref('stg_players_stats') }}
)

select
  team_name,
  player_name,
  position,
  country,
  games,

  win_rate,
  kda,

  case when nullif(avg_deaths, 0) is null then null
       else (avg_kills + avg_assists) / nullif(avg_deaths, 0)
  end as kda_calc,

  (avg_kills + avg_assists) as avg_kills_plus_assists,

  case when nullif((avg_kills + avg_assists), 0) is null then null
       else avg_kills / nullif((avg_kills + avg_assists), 0)
  end as kill_weight_in_contribution,

  dmg_per_min,
  gold_per_min,

  case when nullif(gold_per_min, 0) is null then null
       else dmg_per_min / nullif(gold_per_min, 0)
  end as damage_per_gold_per_min,

  kill_participation,
  team_damage_share,

  (kill_participation * team_damage_share) as teamfight_impact_blend,

  cs_per_min,
  gold_diff_15,
  cs_diff_15,
  xp_diff_15,

  vision_score_per_min,
  wards_placed_per_min,
  wards_cleared_per_min,
  control_wards_per_min,

  case when nullif(wards_placed_per_min, 0) is null then null
       else vision_score_per_min / nullif(wards_placed_per_min, 0)
  end as vision_efficiency_vs_per_ward_per_min,

  first_blood_pct,
  first_blood_victim_pct,
  (first_blood_pct - first_blood_victim_pct) as first_blood_net,

  solo_kills,
  penta_kills
from p
