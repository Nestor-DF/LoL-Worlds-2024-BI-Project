{{ config(materialized='table') }}

with g as (
  select * from {{ ref('stg_player_games') }}
),

team_game as (
  select
    team_match_key,
    match_key,
    team_name,
    opponent_team_name,
    max(is_win) as is_win,

    max(kills_team)   as kills_team,
    max(turrets_team) as turrets_team,
    max(dragons_team) as dragons_team,
    max(barons_team)  as barons_team,

    sum(gold_diff_15) as gold_diff_15_sum
  from g
  where is_win is not null
  group by team_match_key, match_key, team_name, opponent_team_name
),

with_opp as (
  select
    a.*,
    b.kills_team   as kills_opp,
    b.turrets_team as turrets_opp,
    b.dragons_team as dragons_opp,
    b.barons_team  as barons_opp,
    b.gold_diff_15_sum as gold_diff_15_sum_opp
  from team_game a
  left join team_game b
    on a.match_key = b.match_key
   and a.team_name = b.opponent_team_name
   and a.opponent_team_name = b.team_name
),

x as (
  select
    cast(is_win as {{ dbt.type_numeric() }}) as y,

    cast((kills_team - kills_opp) as {{ dbt.type_numeric() }}) as kills_diff,
    cast((turrets_team - turrets_opp) as {{ dbt.type_numeric() }}) as turrets_diff,
    cast((dragons_team - dragons_opp) as {{ dbt.type_numeric() }}) as dragons_diff,
    cast((barons_team - barons_opp) as {{ dbt.type_numeric() }}) as barons_diff,
    cast((gold_diff_15_sum - gold_diff_15_sum_opp) as {{ dbt.type_numeric() }}) as gold15_diff
  from with_opp
),

corrs as (
  select 'gold15_diff' as metric,
    (avg(gold15_diff * y) - avg(gold15_diff) * avg(y))
    / nullif(stddev_samp(gold15_diff) * stddev_samp(y), 0) as corr
  from x

  union all
  select 'kills_diff',
    (avg(kills_diff * y) - avg(kills_diff) * avg(y))
    / nullif(stddev_samp(kills_diff) * stddev_samp(y), 0)
  from x

  union all
  select 'turrets_diff',
    (avg(turrets_diff * y) - avg(turrets_diff) * avg(y))
    / nullif(stddev_samp(turrets_diff) * stddev_samp(y), 0)
  from x

  union all
  select 'dragons_diff',
    (avg(dragons_diff * y) - avg(dragons_diff) * avg(y))
    / nullif(stddev_samp(dragons_diff) * stddev_samp(y), 0)
  from x

  union all
  select 'barons_diff',
    (avg(barons_diff * y) - avg(barons_diff) * avg(y))
    / nullif(stddev_samp(barons_diff) * stddev_samp(y), 0)
  from x
)

select
  metric,
  corr
from corrs
order by abs(corr) desc
