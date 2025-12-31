{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'players_stats') }}
),

renamed as (
    select
        teamname   as team_name,
        playername as player_name,
        position   as position,

        cast(games as integer) as games,

        cast(win_rate as {{ dbt.type_numeric() }}) as win_rate,
        cast(kda      as {{ dbt.type_numeric() }}) as kda,

        cast(avg_kills   as {{ dbt.type_numeric() }}) as avg_kills,
        cast(avg_deaths  as {{ dbt.type_numeric() }}) as avg_deaths,
        cast(avg_assists as {{ dbt.type_numeric() }}) as avg_assists,

        cast(cspermin   as {{ dbt.type_numeric() }}) as cs_per_min,
        cast(goldpermin as {{ dbt.type_numeric() }}) as gold_per_min,

        cast(kp             as {{ dbt.type_numeric() }}) as kill_participation,
        cast(damagepercent  as {{ dbt.type_numeric() }}) as team_damage_share,
        cast(dpm            as {{ dbt.type_numeric() }}) as dmg_per_min,

        cast(vspm     as {{ dbt.type_numeric() }}) as vision_score_per_min,
        cast(avg_wpm  as {{ dbt.type_numeric() }}) as wards_placed_per_min,
        cast(avg_wcpm as {{ dbt.type_numeric() }}) as wards_cleared_per_min,
        cast(avg_vwpm as {{ dbt.type_numeric() }}) as control_wards_per_min,

        cast(gd_15  as {{ dbt.type_numeric() }}) as gold_diff_15,
        cast(csd_15 as {{ dbt.type_numeric() }}) as cs_diff_15,
        cast(xpd_15 as {{ dbt.type_numeric() }}) as xp_diff_15,

        cast(fb        as {{ dbt.type_numeric() }}) as first_blood_pct,
        cast(fb_victim as {{ dbt.type_numeric() }}) as first_blood_victim_pct,

        cast(penta_kills as integer) as penta_kills,
        cast(nullif(solo_kills,  '-') as integer) as solo_kills,

        country            as country,
        upper(flashkeybind) as flash_keybind
    from source
)

select * from renamed
