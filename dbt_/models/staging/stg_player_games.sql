{{ config(materialized='view') }}

with source as (
  select * from {{ source('raw', 'players_teams_stats_extended') }}
),

typed as (
  select
    to_date(date, 'DD.MM.YYYY') as match_date,
    stage,
    round,
    patch,
    cast(no_game as integer) as game_number,

    team          as team_name,
    opponent_team as opponent_team_name,

    player     as player_name,
    upper(role) as role,
    champion,

    case
      when lower(outcome) in ('win','w','victory')  then 1
      when lower(outcome) in ('loss','l','defeat')  then 0
      else null
    end as is_win,

    cast(kills_team   as integer) as kills_team,
    cast(turrets_team as integer) as turrets_team,
    cast(dragon_team  as integer) as dragons_team,
    cast(baron_team   as integer) as barons_team,

    cast(gd_15  as {{ dbt.type_numeric() }}) as gold_diff_15,
    cast(csd_15 as {{ dbt.type_numeric() }}) as cs_diff_15,
    cast(xpd_15 as {{ dbt.type_numeric() }}) as xp_diff_15,

    cast(dpm as {{ dbt.type_numeric() }}) as dmg_per_min,
    cast(kp as {{ dbt.type_numeric() }}) as kill_participation,
    cast(vspm as {{ dbt.type_numeric() }}) as vision_score_per_min
  from source
),

keys as (
  select
    *,
    concat(
      cast(match_date as {{ dbt.type_string() }}), '|',
      coalesce(stage,''), '|',
      coalesce(round,''), '|',
      cast(game_number as {{ dbt.type_string() }}), '|',
      case when team_name < opponent_team_name then team_name else opponent_team_name end, '|',
      case when team_name < opponent_team_name then opponent_team_name else team_name end
    ) as match_key,

    concat(
      concat(
        cast(match_date as {{ dbt.type_string() }}), '|',
        coalesce(stage,''), '|',
        coalesce(round,''), '|',
        cast(game_number as {{ dbt.type_string() }}), '|',
        case when team_name < opponent_team_name then team_name else opponent_team_name end, '|',
        case when team_name < opponent_team_name then opponent_team_name else team_name end
      ),
      '|', team_name
    ) as team_match_key
  from typed
)

select * from keys
