{{ config(materialized='table') }}

with g as (
  select * from {{ ref('stg_player_games') }}
),

team_games as (
  select
    team_match_key,
    match_key,
    match_date,
    stage,
    round,
    patch,
    team_name,
    opponent_team_name,

    max(case when role = 'TOP'     then champion end) as champ_top,
    max(case when role = 'JUNGLE'  then champion end) as champ_jungle,
    max(case when role = 'MID'     then champion end) as champ_mid,
    max(case when role = 'ADCARRY'     then champion end) as champ_adc,
    max(case when role = 'SUPPORT' then champion end) as champ_support,

    max(is_win) as is_win
  from g
  group by
    team_match_key, match_key, match_date, stage, round, patch, team_name, opponent_team_name
)

select
  composition_signature,
  champ_top,
  champ_jungle,
  champ_mid,
  champ_adc,
  champ_support,

  count(*) as games_played,
  avg(is_win) as win_rate
from (
  select
    *,
    concat(
      coalesce(champ_top,'?'), '|',
      coalesce(champ_jungle,'?'), '|',
      coalesce(champ_mid,'?'), '|',
      coalesce(champ_adc,'?'), '|',
      coalesce(champ_support,'?')
    ) as composition_signature
  from team_games
) x
where is_win is not null
group by
  composition_signature, champ_top, champ_jungle, champ_mid, champ_adc, champ_support
