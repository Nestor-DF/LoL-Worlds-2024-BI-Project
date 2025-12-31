{{ config(materialized='table') }}

with g as (
  select * from {{ ref('stg_player_games') }}
),

team_game as (
  select
    team_match_key,
    match_key,
    match_date,
    stage,
    round,
    patch,
    team_name,
    opponent_team_name,

    max(is_win) as is_win,

    max(kills_team)   as kills_team,
    max(turrets_team) as turrets_team,
    max(dragons_team) as dragons_team,
    max(barons_team)  as barons_team,

    sum(gold_diff_15) as gold_diff_15_sum,
    sum(cs_diff_15)   as cs_diff_15_sum,
    sum(xp_diff_15)   as xp_diff_15_sum
  from g
  where is_win is not null
  group by
    team_match_key, match_key, match_date, stage, round, patch, team_name, opponent_team_name
),

with_opp as (
  select
    a.*,
    b.kills_team   as kills_opp,
    b.turrets_team as turrets_opp,
    b.dragons_team as dragons_opp,
    b.barons_team  as barons_opp,

    b.gold_diff_15_sum as gold_diff_15_sum_opp,
    b.cs_diff_15_sum   as cs_diff_15_sum_opp,
    b.xp_diff_15_sum   as xp_diff_15_sum_opp
  from team_game a
  left join team_game b
    on a.match_key = b.match_key
   and a.team_name = b.opponent_team_name
   and a.opponent_team_name = b.team_name
),

per_game_kpis as (
  select
    *,
    (kills_team   - kills_opp)   as kills_diff,
    (turrets_team - turrets_opp) as turrets_diff,
    (dragons_team - dragons_opp) as dragons_diff,
    (barons_team  - barons_opp)  as barons_diff,

    (gold_diff_15_sum - gold_diff_15_sum_opp) as gold15_diff_sum_diff,
    (cs_diff_15_sum   - cs_diff_15_sum_opp)   as cs15_diff_sum_diff,
    (xp_diff_15_sum   - xp_diff_15_sum_opp)   as xp15_diff_sum_diff,

    (
      0.30 * (kills_team - kills_opp)
      + 0.50 * (turrets_team - turrets_opp)
      + 1.00 * (dragons_team - dragons_opp)
      + 2.00 * (barons_team - barons_opp)
    ) as objective_domination_index
  from with_opp
),

agg_stage as (
  select
    stage,
    team_name,

    count(*) as games_played,
    sum(is_win) as wins,
    avg(is_win) as win_rate,

    avg(objective_domination_index) as objective_domination_index_avg,

    avg(gold15_diff_sum_diff) as gold15_diff_sum_avg,
    stddev_samp(gold15_diff_sum_diff) as gold15_diff_sum_std,

    avg(kills_diff) as kills_diff_avg,
    stddev_samp(kills_diff) as kills_diff_std,

    avg(case when gold15_diff_sum_diff > 0 then 1 else 0 end) as pct_ahead_at_15,
    avg(case when gold15_diff_sum_diff > 0 and is_win = 1 then 1 else 0 end) as pct_win_when_ahead_15
  from per_game_kpis
  group by stage, team_name
),

agg_all as (
  select
    'ALL' as stage,
    team_name,

    count(*) as games_played,
    sum(is_win) as wins,
    avg(is_win) as win_rate,

    avg(objective_domination_index) as objective_domination_index_avg,

    avg(gold15_diff_sum_diff) as gold15_diff_sum_avg,
    stddev_samp(gold15_diff_sum_diff) as gold15_diff_sum_std,

    avg(kills_diff) as kills_diff_avg,
    stddev_samp(kills_diff) as kills_diff_std,

    avg(case when gold15_diff_sum_diff > 0 then 1 else 0 end) as pct_ahead_at_15,
    avg(case when gold15_diff_sum_diff > 0 and is_win = 1 then 1 else 0 end) as pct_win_when_ahead_15
  from per_game_kpis
  group by team_name
)

select * from agg_stage
union all
select * from agg_all
